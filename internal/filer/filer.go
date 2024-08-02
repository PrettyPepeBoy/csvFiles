package filer

import (
	"bytes"
	"encoding/csv"
	"errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"log"
	"os"
	"strconv"
)

func (s *Storage) LoadAllData() error {
	files, err := os.ReadDir(viper.GetString("csv-files-directory"))
	if err != nil {
		logrus.Errorf("failed to read directory: %s, error: %v", viper.GetString("csv-files-directory"), err)
		return err
	}

	for _, file := range files {
		if err = s.loadFileData(file); err != nil {
			return err
		}
	}

	return nil
}

var (
	ErrNewFileIsNotSet = errors.New("new file is not set")
	ErrMustBeUnique    = errors.New("id must be unique")
)

func (s *Storage) WriteData(fileName string, id []int, newFile, notUnique bool) error {
	_, err := os.Stat(viper.GetString("csv-files-directory") + fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && newFile {
		} else if errors.Is(err, os.ErrNotExist) && !newFile {
			return ErrNewFileIsNotSet
		} else {
			logrus.Errorf("failed to put data in file, error: %v", err)
			return err
		}
	}

	if newFile {
		s.initFile(fileName)
	}

	return s.writeData(fileName, id, notUnique)
}

func (s *Storage) writeData(filename string, id []int, notUnique bool) error {
	slc := make([]string, len(id))

	for i, elem := range id {
		if !s.add(elem, filename, notUnique) {
			return ErrMustBeUnique
		}

		slc[i] = strconv.Itoa(elem)
	}

	f, err := os.OpenFile(viper.GetString("csv-files-directory")+filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		logrus.Errorf("failed to open file: %s, error: %v", filename, err)
		return err
	}

	writer := csv.NewWriter(f)
	err = writer.Write(slc)
	if err != nil {
		logrus.Errorf("failed to write data to file, error: %v", err)
		return err
	}

	writer.Flush()
	_ = f.Close()
	return nil
}

func (s *Storage) GetData(filename string) ([]byte, error) {
	data := s.getData(filename)
	var buf bytes.Buffer
	for _, elem := range data {
		byteId := []byte(strconv.Itoa(elem))
		buf.Grow(len(byteId))
		_, err := buf.Write(byteId)
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (s *Storage) loadFileData(file os.DirEntry) error {
	f, err := os.Open(viper.GetString("csv-files-directory") + file.Name())
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
