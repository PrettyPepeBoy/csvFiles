package filer

import (
	"encoding/csv"
	"errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/valyala/fasthttp"
	"log"
	"os"
	"strconv"
	"time"
)

type file struct {
	directory string
}

var (
	csvFiles *file

	ErrNewFileIsNotSet = errors.New("new file is not set")
	ErrMustBeUnique    = errors.New("id must be unique")
)

func initFile() *file {
	return &file{
		directory: viper.GetString("storage.files.directory"),
	}
}

func (s *Storage) WriteData(filename string, id []int, newFile, notUnique bool) error {
	t := time.Now()
	defer timeMetric("api/v1/ids", fasthttp.MethodPut, t)

	_, err := os.Stat(csvFiles.directory + filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && newFile {
			s.initFile(filename)
		} else if errors.Is(err, os.ErrNotExist) && !newFile {
			metricFilerErrors.Inc()
			return ErrNewFileIsNotSet
		} else {
			metricFilerErrors.Inc()
			logrus.Errorf("failed to put data in file, error: %v", err)
			return err
		}
	}
	return s.writeData(filename, id, notUnique)
}

func (s *Storage) writeData(filename string, ids []int, notUnique bool) error {
	buf := make([]string, len(ids))

	for i, id := range ids {
		if !s.add(id, filename, notUnique) {
			return ErrMustBeUnique
		}

		buf[i] = strconv.Itoa(id)
	}

	f, err := os.OpenFile(csvFiles.directory+filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {

		metricFilerErrors.Inc()
		logrus.Errorf("failed to open file: %s, error: %v", filename, err)
		return err
	}

	writer := csv.NewWriter(f)
	err = writer.Write(buf)
	if err != nil {
		metricFilerErrors.Inc()
		logrus.Errorf("failed to write data to file, error: %v", err)
		return err
	}

	writer.Flush()
	_ = f.Close()
	return nil
}

func (s *Storage) GetData(filename string) ([]int, error) {
	return s.getData(filename), nil
}

func (s *Storage) LoadAllData() error {
	csvFiles = initFile()

	files, err := os.ReadDir(csvFiles.directory)
	if err != nil {
		logrus.Errorf("failed to read directory: %s, error: %v", viper.GetString("csv-files-directory"), err)
		return err
	}

	for _, f := range files {
		if err = s.loadFileData(f); err != nil {
			return err
		}
	}

	return nil
}

func (s *Storage) loadFileData(file os.DirEntry) error {
	f, err := os.Open(csvFiles.directory + file.Name())
	if err != nil {
		logrus.Errorf("failed to open file: %s, error: %v", file.Name(), err)
		return err
	}

	s.initFile(file.Name())

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1

	data, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("failed to read data from csv file, error: %v", err)
		return err
	}

	for _, array := range data {
		var idInt int
		for _, id := range array {
			idInt, err = strconv.Atoi(id)
			if err != nil {
				log.Fatalf("failed to convert string to integer in file %s, element: %v, error: %v", file.Name(), id, err)
			}

			s.loadData(idInt, file.Name())
		}
	}

	_ = f.Close()
	return nil
}

func (s *Storage) initFile(filename string) {
	s.fileStorage[filename] = &Data{
		id: make(map[int]struct{}),
	}
}
