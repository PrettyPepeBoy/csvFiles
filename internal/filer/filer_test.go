package filer

import (
	"bufio"
	"github.com/stretchr/testify/require"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestFiler_DeleteData(t *testing.T) {
	var ids = []uint32{1, 2, 3, 4, 5}
	writeData("file_1_test.csv", ids)

	f, err := newFilerTest()
	require.NoError(t, err, "should init newFiler without an error")

	var deleteIds = []uint32{3, 6, 5}
	err = f.DeleteData("file_1_test.csv", deleteIds)
	require.NoError(t, err, "should successfully delete all data")

	currentIds := f.getData("file_1_test.csv")
	require.Len(t, currentIds, 3, "after deleting only 3 ids should be in storage")

	deleteIds = []uint32{1, 2, 4}
	err = f.DeleteData("file_1_test.csv", deleteIds)
	require.NoError(t, err, "should successfully delete all data")

	_, err = os.Open(f.directory + "file_1_test.csv")
	require.ErrorIs(t, err, os.ErrNotExist, "file should not exist")
}

func TestFiler_WriteData(t *testing.T) {
	var ids = []uint32{1, 2, 3, 4, 5}
	writeData("file_1_test.csv", ids)

	f, err := newFilerTest()
	require.NoError(t, err, "should init newFiler without an error")

	var newIds = []uint32{6, 7, 8}
	err = f.WriteData("file_1_test.csv", newIds, false, false)
	require.NoError(t, err, "should write new data without error")

	var idsForNewFile = []uint32{9, 10, 11}
	err = f.WriteData("file_2_test.csv", idsForNewFile, true, false)
	require.NoError(t, err, "should write new ids in new file without error")

	idsForNewFile = []uint32{12, 13, 14}
	err = f.WriteData("file_3_test.csv", idsForNewFile, false, false)
	require.ErrorIs(t, err, ErrNewFileIsNotSet, "to create new file should specify flag NewFile")
}

func TestFiler_WriteAndDeleteDoubleData(t *testing.T) {
	var ids = []uint32{1, 2, 3, 4, 5}
	writeData("file_1_test.csv", ids)

	f, err := newFilerTest()
	require.NoError(t, err, "should init newFiler without an error")

	var newIds = []uint32{9, 2, 10}
	err = f.WriteData("file_1_test.csv", newIds, false, false)
	require.ErrorIs(t, err, ErrMustBeUnique, "id 2 is already exist in file, so it can't be put in file without flag notUnique")

	err = f.WriteData("file_1_test.csv", newIds, false, true)
	require.NoError(t, err, "should be successfully put with flag unique")

	data, err := f.GetData("file_1_test.csv")
	require.NoError(t, err, "should successfully get all data")
	require.Len(t, data, 7, "there are 7 different ids in file right now")

	require.Equal(t, 2, int(f.storage["file_1_test.csv"][2]), "value must be equal to 2")

	var deleteId = []uint32{2}
	err = f.DeleteData("file_1_test.csv", deleteId)
	require.NoError(t, err, "must successfully delete data from file")

	data, err = f.GetData("file_1_test.csv")
	require.NoError(t, err, "should successfully get all data")
	require.Len(t, data, 7, "id with value 2 must be stored in storage, because before delete there was 2 ids with such value")

	deleteId = []uint32{2, 9}
	err = f.DeleteData("file_1_test.csv", deleteId)
	require.NoError(t, err, "must successfully delete data from file")
	data, err = f.GetData("file_1_test.csv")
	require.NoError(t, err, "should successfully get all data")
	require.Len(t, data, 5, "ids with values 2 and 9 is not exist anymore")
}

func writeData(filename string, ids []uint32) {
	file, err := os.OpenFile("./.csv_files_test/"+filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0777)
	if err != nil {
		panic(err)
	}

	slc := make([]string, 0, len(ids))

	for _, id := range ids {
		slc = append(slc, strconv.Itoa(int(id)))
	}

	str := strings.Join(slc, ",")

	writer := bufio.NewWriter(file)
	_, err = writer.WriteString(str)
	if err != nil {
		panic(err)
	}

	_ = writer.Flush()
	_ = file.Close()
}

func newFilerTest() (*Filer, error) {
	f := &Filer{
		storage:   make(map[string]map[uint32]uint8),
		directory: "./.csv_files_test/",
	}

	err := f.loadAllData()
	if err != nil {
		return nil, err
	}

	return f, nil
}
