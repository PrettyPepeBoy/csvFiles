package filer

import (
	"encoding/csv"
	"errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"log"
	"os"
	"slices"
	"strconv"
)

type Filer struct {
	storage   *storage
	directory string
}

var (
	ErrNewFileIsNotSet = errors.New("new file is not set")
	ErrMustBeUnique    = errors.New("id must be unique")
	ErrFileIsNotExist  = errors.New("file is not exist")
)

// NewFiler creates new filer service
func NewFiler() (*Filer, error) {
	f := &Filer{
		storage:   newStorage(),
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
func (f *Filer) WriteData(filename string, ids []int, newFile, notUnique bool) error {
	_, err := os.Stat(f.directory + filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && newFile {
			f.initFile(filename)
		} else if errors.Is(err, os.ErrNotExist) && !newFile {
			metricFilerErrors.Inc()
			return ErrNewFileIsNotSet
		} else {
			metricFilerErrors.Inc()
			logrus.Errorf("failed to put data in file, error: %v", err)
			return err
		}
	}
	return f.writeData(filename, ids, notUnique)
}

func (f *Filer) writeData(filename string, ids []int, notUnique bool) error {
	buf := make([]string, len(ids))

	for i, id := range ids {
		if !f.storage.add(id, filename, notUnique) {
			return ErrMustBeUnique
		}

		buf[i] = strconv.Itoa(id)
	}

	file, err := os.OpenFile(f.directory+filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {

		metricFilerErrors.Inc()
		logrus.Errorf("failed to open file: %s, error: %v", filename, err)
		return err
	}

	writer := csv.NewWriter(file)
	err = writer.Write(buf)
	if err != nil {
		metricFilerErrors.Inc()
		logrus.Errorf("failed to write data to file, error: %v", err)
		return err
	}

	writer.Flush()
	_ = file.Close()
	return nil
}

// GetData returns set of ids from storage.
// To get ids from storage you should specify
// file, from which you want to get data.
// If file specified was not found in storage,
// it returns error that file is not exist.
func (f *Filer) GetData(filename string) ([]int, error) {
	if dt, ok := f.storage.getData(filename); !ok {
		return nil, ErrFileIsNotExist
	} else {
		return dt, nil
	}
}

// DeleteData deletes set of ids, which is given in function body
// from file, which name is also given by function body.
// If file is not exist, function returns error that file
// is not exist. DeleteData also deletes data from storage.
func (f *Filer) DeleteData(filename string, ids []int) error {
	file, err := os.OpenFile(f.directory+filename, os.O_RDONLY, 0644)
	if err != nil {
		if errors.Is(os.ErrNotExist, err) {
			return ErrFileIsNotExist
		}
		logrus.Errorf("failed to open file: %s, error: %v", filename, err)
		return err
	}

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	dt, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("failed to read data from csv file, error: %v", err)
		return err
	}
	_ = file.Close()

	err = os.Truncate(f.directory+filename, 0)
	if err != nil {
		logrus.Errorf("failed to remove file, error: %v", err)
		return err
	}

	result := make([][]string, len(dt))
	for i, slc := range dt {
		length := len(slc)

		for _, id := range slc {
			idInt, err := strconv.Atoi(id)
			if err != nil {
				return err
			}

			if _, find := slices.BinarySearch(ids, idInt); find {
				length -= 1
				continue
			}
			result[i] = append(result[i], id)
		}
		result[i] = result[i][:length:length]
	}

	file, err = os.Create(f.directory + filename)
	if err != nil {
		metricFilerErrors.Inc()
		panic(err)
	}

	writer := csv.NewWriter(file)
	err = writer.WriteAll(result)
	if err != nil {
		metricFilerErrors.Inc()
		logrus.Errorf("failed to write data, error: %v", err)
		return err
	}

	f.storage.deleteData(filename, ids)

	writer.Flush()
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

	f.initFile(file.Name())

	reader := csv.NewReader(fl)
	reader.FieldsPerRecord = -1

	dt, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("failed to read data from csv file, error: %v", err)
		return err
	}

	for _, array := range dt {
		var idInt int
		for _, id := range array {
			idInt, err = strconv.Atoi(id)
			if err != nil {
				log.Fatalf("failed to convert string to integer in file %s, element: %v, error: %v", file.Name(), id, err)
			}

			f.storage.loadData(idInt, file.Name())
		}
	}

	_ = fl.Close()
	return nil
}

func (f *Filer) initFile(filename string) {
	f.storage.fileStorage[filename] = &data{
		id: make(map[int]struct{}),
	}
}
