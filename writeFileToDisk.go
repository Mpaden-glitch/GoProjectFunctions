package builtin

import (
	"bitbucket.org/primelogic_io/bitlantern/service/dataflow"
	_ "github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
	"io"
	"os"
)

func init() {
	//Register function with default builtin.FunctionProvider
	DefaultInstance().RegisterFunction(
		Function{
			FunctionSpec: dataflow.FunctionSpec{
				Key:           "writeFileToDisk",
				Name:          "Write File to Disk",
				Description:   "Writes all input files into the destinationFolder",
				Category:      "File",
				ExecutionMode: "sync",
				InputPorts:    nil,
				OutputPorts:   nil,
			},
			NewFunction: func() dataflow.Function {
				return &(writeFileToDisk{})
			},
		})
}

type writeFileToDisk struct {
	config writeFileToDiskConfig
}

type writeFileToDiskConfig struct {
	destFolder string
}

// buildConfig builds a splitConfig from the passed in map. The map must be in the form:
// {
//		"destinationFolder": "/Users/bitlantern/output"
// }
//
func (f *writeFileToDisk) buildConfig(config map[string]interface{}) (writeFileToDiskConfig, error) {
	c := writeFileToDiskConfig{}

	c.destFolder = config["destinationFolder"].(string)

	return c, nil
}

func (f *writeFileToDisk) Execute(in dataflow.InputReader, out dataflow.OutputWriter, config map[string]interface{}) error {
	defer out.Close()

	//Parse/read config options
	parsedConfig, err := f.buildConfig(config)
	if err != nil {
		panic("Error parsing function config")
	}
	f.config = parsedConfig

	//Open the data stream
	reader := in.PortReader(dataflow.DEFAULT_INPUT_PORT_NAME)
	err = reader.Open()
	if err != nil {
		panic("Unable to read record from input")
	}

	//For each file, parse the records and output them
	for reader.HasNext() {
		curEntry, err := reader.Next()
		if err != nil {
			panic(err)
		}

		curFile, err := curEntry.GetAsFile()
		if err != nil {
			panic(err)
		}

		f.writeFile(curFile)
	}

	return nil
}

func (f *writeFileToDisk) writeFile(file dataflow.File) {
	fileReader := file.Reader()

	diskFile, err := os.Create(f.config.destFolder + "/" + file.Filename())
	defer diskFile.Close()
	if err != nil {
		panic(err)
	}

	io.Copy(diskFile, fileReader)
}
