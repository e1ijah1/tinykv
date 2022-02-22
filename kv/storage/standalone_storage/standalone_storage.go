package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	conf *config.Config
	wb   *engine_util.WriteBatch
	db   *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		conf: conf,
		wb:   &engine_util.WriteBatch{},
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	var err error
	opts := badger.DefaultOptions
	opts.Dir = s.conf.DBPath
	opts.ValueDir = s.conf.DBPath
	if s.db == nil {
		s.db, err = badger.Open(opts)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return s, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	for _, b := range batch {
		switch data := b.Data.(type) {
		case storage.Put:
			s.wb.SetCF(data.Cf, data.Key, data.Value)
		case storage.Delete:
			s.wb.DeleteCF(data.Cf, data.Key)
		}
	}
	err := s.wb.WriteToDB(s.db)
	s.wb.Reset()
	return err
}

func (s *StandAloneStorage) GetCF(cf string, key []byte) ([]byte, error) {
	var val []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(engine_util.KeyWithCF(cf, key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		val, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (s *StandAloneStorage) IterCF(cf string) engine_util.DBIterator {
	txn := s.db.NewTransaction(false)
	return engine_util.NewCFIterator(cf, txn)
}

func (s *StandAloneStorage) Close() {

}
