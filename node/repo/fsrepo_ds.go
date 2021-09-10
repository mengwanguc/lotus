package repo

import (
	"context"
	"os"
	"path/filepath"

	dgbadger "github.com/dgraph-io/badger/v2"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"
	"golang.org/x/xerrors"

	"github.com/ipfs/go-datastore"
	badger "github.com/ipfs/go-ds-badger2"
	levelds "github.com/ipfs/go-ds-leveldb"
	measure "github.com/ipfs/go-ds-measure"
	mds "github.com/mengwanguc/go-ds-motr/mds"
	mio "github.com/mengwanguc/go-ds-motr/mio"

	"strings"
)

type dsCtor func(path string, readonly bool) (datastore.Batching, error)

var fsDatastores = map[string]dsCtor{
//	"metadata": levelDs,
	"metadata": motrDs,
	
	

	// Those need to be fast for large writes... but also need a really good GC :c
//	"staging": badgerDs, // miner specific
	"staging": motrDs, // miner specific

//	"client": badgerDs, // client specific
	"client": motrDs, // client specific
}

func badgerDs(path string, readonly bool) (datastore.Batching, error) {
	opts := badger.DefaultOptions
	opts.ReadOnly = readonly

	opts.Options = dgbadger.DefaultOptions("").WithTruncate(true).
		WithValueThreshold(1 << 10)
	return badger.NewDatastore(path, &opts)
}

func levelDs(path string, readonly bool) (datastore.Batching, error) {
	return levelds.NewDatastore(path, &levelds.Options{
		Compression: ldbopts.NoCompression,
		NoSync:      false,
		Strict:      ldbopts.StrictAll,
		ReadOnly:    readonly,
	})
}



var mioConf mio.Config = mio.Config{
	        LocalEP:    "172.31.36.67@tcp:12345:33:1000",
        	HaxEP:      "172.31.36.67@tcp:12345:34:1",
        	Profile:    "0x7000000000000001:0",
        	ProcFid:    "0x7200000000000001:64",
		TraceOn:    false,
		Verbose:    false,
		ThreadsN:   1,
	}

var metadataIdx = "0x7800000000000001:123456701"
var stagingIdx = "0x7800000000000001:123456702"
var clientIdx = "0x7800000000000001:123456703"


func motrDs(path string, readonly bool) (datastore.Batching, error) {
	if strings.Contains(path, "metadata") {
		return mds.Open(mioConf, metadataIdx)
	} else if strings.Contains(path, "staging") {
		return mds.Open(mioConf, stagingIdx)
	} else {
		return mds.Open(mioConf, clientIdx)
	}
}

func (fsr *fsLockedRepo) openDatastores(readonly bool) (map[string]datastore.Batching, error) {
	if err := os.MkdirAll(fsr.join(fsDatastore), 0755); err != nil {
		return nil, xerrors.Errorf("mkdir %s: %w", fsr.join(fsDatastore), err)
	}

	out := map[string]datastore.Batching{}

	for p, ctor := range fsDatastores {
		prefix := datastore.NewKey(p)

		// TODO: optimization: don't init datastores we don't need
		ds, err := ctor(fsr.join(filepath.Join(fsDatastore, p)), readonly)
		if err != nil {
			return nil, xerrors.Errorf("opening datastore %s: %w", prefix, err)
		}

		ds = measure.New("fsrepo."+p, ds)

		out[datastore.NewKey(p).String()] = ds
	}

	return out, nil
}

func (fsr *fsLockedRepo) Datastore(_ context.Context, ns string) (datastore.Batching, error) {
	fsr.dsOnce.Do(func() {
		fsr.ds, fsr.dsErr = fsr.openDatastores(fsr.readonly)
	})

	if fsr.dsErr != nil {
		return nil, fsr.dsErr
	}
	ds, ok := fsr.ds[ns]
	if ok {
		return ds, nil
	}
	return nil, xerrors.Errorf("no such datastore: %s", ns)
}
