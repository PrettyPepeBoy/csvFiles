package filer

import (
	"bufio"
	"errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Filer struct {
	storage   map[string]map[uint32]uint8
	directory string
	mx        sync.Mutex
}

var (
	ErrNewFileIsNotSet = errors.New("new file is not set")
	ErrMustBeUnique    = errors.New("id must be unique")
	ErrFileIsNotExist  = errors.New("file is not exist")
)

// NewFiler creates new filer service
func NewFiler() (*Filer, error) {
	f := &Filer{
		storage:   make(map[string]map[uint32]uint8),
		directory: viper.GetString("storage.files.directory"),
	}

	err := f.loadAllData()
	if err != nil {
		return nil, err
	}

	return f, nil
}

// WriteData is using set of ids to write in file
// whose name is set by filename.
// If flag newFile is set `true` method create new file
// to write data in.
// You shouldn't set newFile flag `true` if you do not want
// to overwrite data in already existing file
// If flag notUnique is set `true`, it allows user to set already
// existing ids in file.
func (f *Filer) WriteData(filename string, ids []uint32, newFile, notUnique bool) error {
	f.mx.Lock()
	defer f.mx.Unlock()

	if newFile {
		f.initFile(filename)
	} else {
		_, ok := f.storage[filename]
		if !ok {
			return ErrNewFileIsNotSet
		}
	}

	return f.writeData(filename, ids, newFile, notUnique)
}

func (f *Filer) writeData(filename string, ids []uint32, newFile, notUnique bool) error {

	buf := make([]string, len(ids))

	for i, id := range ids {
		if !f.add(id, filename, notUnique) {
			f.deleteData(filename, ids)
			return ErrMustBeUnique
		}

		buf[i] = strconv.Itoa(int(id))
	}

	file, err := os.OpenFile(f.directory+filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0777)
	if err != nil {
		f.deleteData(filename, ids)
		_ = file.Close()
		metricFilerErrors.Inc()
		return err
	}

	writer := bufio.NewWriter(file)

	var data string
	if newFile {
		data = strings.Join(buf, ",")
	} else {
		data = "," + strings.Join(buf, ",")
	}

	_, err = writer.Write([]byte(data))
	if err != nil {
		f.deleteData(filename, ids)
		metricFilerErrors.Inc()
		logrus.Errorf("failed to write data to file, error: %v", err)
		return err
	}

	_ = writer.Flush()
	_ = file.Close()
	return nil
}

// GetData returns set of ids from storage.
// To get ids from storage you should specify
// file, from which you want to get data.
// If file specified was not found in storage,
// it returns error that file is not exist.
func (f *Filer) GetData(filename string) ([]uint32, error) {
	f.mx.Lock()
	defer f.mx.Unlock()
	_, ok := f.storage[filename]
	if !ok {
		return nil, ErrFileIsNotExist
	}

	ids := f.getData(filename)
	return ids, nil
}

// DeleteData deletes set of ids, which is given in function body
// from file, which name is also given by function body.
// If file is not exist, function returns error that file
// is not exist. DeleteData also deletes data from storage.
// If there is no data in file after DeleteData, it also deletes
// file from current directory.
func (f *Filer) DeleteData(filename string, ids []uint32) error {
	f.mx.Lock()
	defer f.mx.Unlock()

	_, ok := f.storage[filename]
	if !ok {
		return ErrFileIsNotExist
	}

	f.deleteData(filename, ids)

	file, err := os.OpenFile(f.directory+filename, os.O_TRUNC|os.O_WRONLY, 0777)
	if err != nil {
		if errors.Is(os.ErrNotExist, err) {
			return ErrFileIsNotExist
		}
		logrus.Errorf("failed to open file: %s, error: %v", filename, err)
		return err
	}

	m := f.storage[filename]
	writer := bufio.NewWriter(file)
	if len(m) == 0 {
		_ = file.Close()
		err = f.DeleteFile(filename)
		if err != nil {
			panic(err)
		}
		return nil
	}

	buf := make([]string, 0, len(m))
	for id := range m {
		buf = append(buf, strconv.Itoa(int(id)))
	}

	data := strings.Join(buf, ",")
	_, err = writer.Write([]byte(data))
	if err != nil {
		f.put(filename, ids)
		logrus.Errorf("failed to write data, error: %v", err)
		panic(err)
	}

	_ = writer.Flush()
	_ = file.Close()
	return nil
}

func (f *Filer) loadAllData() error {
	files, err := os.ReadDir(f.directory)
	if err != nil {
		logrus.Errorf("failed to read directory: %s, error: %v", viper.GetString("csv-files-directory"), err)
		return err
	}

	for _, file := range files {
		if err = f.loadFileData(file); err != nil {
			return err
		}
	}

	return nil
}

func (f *Filer) loadFileData(file os.DirEntry) error {
	fl, err := os.Open(f.directory + file.Name())
	if err != nil {
		logrus.Errorf("failed to open file: %s, error: %v", file.Name(), err)
		return err
	}

	scanner := bufio.NewScanner(fl)
	scanner.Scan()

	if len(scanner.Bytes()) == 0 {
		err = os.Remove(f.directory + file.Name())
		if err != nil {
			panic(err)
		}
	}

	f.initFile(file.Name())
	ids := strings.Split(string(scanner.Bytes()), `,`)
	idsInt := make([]uint32, len(ids))
	for i, id := range ids {
		idInt, err := strconv.Atoi(id)
		if err != nil {
			log.Fatalf("failed to convert string to integer in file %s, element: %v, error: %v", file.Name(), id, err)
			return err
		}

		idsInt[i] = uint32(idInt)
	}

	f.put(file.Name(), idsInt)
	_ = fl.Close()
	return nil
}

func (f *Filer) DeleteFile(filename string) error {
	err := os.Remove(f.directory + filename)
	if err != nil {
		if errors.Is(os.ErrNotExist, err) {
			return ErrFileIsNotExist
		}

		logrus.Errorf("failed to remove file: %s, error: %v", filename, err)
		return err
	}

	delete(f.storage, filename)
	return nil
}

func (f *Filer) initFile(filename string) {
	m := make(map[uint32]uint8)
	f.storage[filename] = m
}

func (f *Filer) add(id uint32, filename string, notUnique bool) bool {
	if f.find(id) {
		if !notUnique {
			f.storage[filename][id]++
			return false
		}
	}

	f.storage[filename][id]++

	return true
}

func (f *Filer) put(filename string, ids []uint32) {
	for _, id := range ids {
		f.storage[filename][id]++
	}
}

func (f *Filer) find(id uint32) bool {
	for _, m := range f.storage {
		for key := range m {
			if key == id {
				return true
			}
		}
	}
	return false
}

func (f *Filer) getData(filename string) []uint32 {
	id := make([]uint32, 0, len(f.storage[filename]))
	for key := range f.storage[filename] {
		id = append(id, key)
	}

	return id
}

func (f *Filer) deleteData(filename string, ids []uint32) {
	for _, id := range ids {
		_, ok := f.storage[filename][id]
		if !ok {
			continue
		}

		f.storage[filename][id]--

		if f.storage[filename][id] == 0 {
			delete(f.storage[filename], id)
		}
	}
}
