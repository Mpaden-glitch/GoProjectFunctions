package builtin

import (
	"bitbucket.org/primelogic_io/bitlantern/service/dataflow"
	_ "github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
)

func init() {
	//Register function with default builtin.FunctionProvider
	DefaultInstance().RegisterFunction(
		Function{
			FunctionSpec: dataflow.FunctionSpec{
				Key:           "fakeBCC",
				Name:          "Fake BCC",
				Description:   "Fakes a BCC SOAP API call",
				Category:      "API",
				ExecutionMode: "sync",
				InputPorts:    nil,
				OutputPorts:   nil,
			},
			NewFunction: func() dataflow.Function {
				return &(fakeBCC{})
			},
		})
}

type fakeBCC struct {
}

type fakeBCCConfig struct {
	stateField string
}

// The available fields for conditions are: filename, size (in bytes), mime type (future)
func (f *fakeBCC) buildConfig(config map[string]interface{}) (fakeBCCConfig, error) {
	c := fakeBCCConfig{}

	c.stateField = config["stateField"].(string)

	return c, nil
}

func (f *fakeBCC) Execute(in dataflow.InputReader, out dataflow.OutputWriter, config map[string]interface{}) error {
	defer out.Close()

	//Parse/read config options
	configOpts, err := f.buildConfig(config)
	if err != nil {
		panic("Error generating a csv")
	}

	//Open the data stream
	reader := in.PortReader(dataflow.DEFAULT_INPUT_PORT_NAME)
	err = reader.Open()
	if err != nil {
		panic("Unable to read record from input")
	}

	//Loop through all files, routing them based on the conditions
	for reader.HasNext() {
		curEntry, err := reader.Next()
		if err != nil {
			panic(err)
		}

		rec, err := curEntry.GetAsRecord()
		if err != nil {
			panic(err)
		}

		stateVal, ok := rec[configOpts.stateField].(string)

		if !ok || len(stateVal) != 2 {
			rec["error_code"] = 22
		} else {
			rec["error_code"] = 0
		}

		out.WriteRecord(dataflow.DEFAULT_OUTPUT_PORT_NAME, &rec)
	}

	return nil
}
