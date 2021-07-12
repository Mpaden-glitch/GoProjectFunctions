package builtin

import (
	"bitbucket.org/primelogic_io/bitlantern/service/dataflow"
	"encoding/csv"
	"fmt"
	_ "github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
)

func init() {
	//Register function with default builtin.FunctionProvider
	DefaultInstance().RegisterFunction(
		Function{
			FunctionSpec: dataflow.FunctionSpec{
				Key:           "generateCSV",
				Name:          "Writes a file to the data pipeline",
				Description:   "Writes a file to the data pipeline",
				Category:      "Data",
				ExecutionMode: "sync",
				InputPorts:    nil,
				OutputPorts:   nil,
			},
			NewFunction: func() dataflow.Function {
				return &(generateCSV{})
			},
		})
}

type generateCSV struct {
}

type generateCSVJSConfig struct {
	// splits contains the rules for data splitting. The first rule that matches the data will be used.
	rules []transformDataConfig
}

type generateCSVConfig struct {
	header       string
	headerValues []string
	dataFields   []string
	delimiter    string
	filename     string
}

// buildConfig builds a splitConfig from the passed in map. The map must be in the form:
// {
//		"defaultOutputPort": "not poor",
//		"rules": [
//			{
//				"jsCondition": "gender.lowerCase().startsWith('f')",
//				"outputPort": "poor"
//			}
//		]
// }
//
// rules.value can either be a value literal (ex: 100.00, "M", etc.) or a reference to another field in the record.
// rules.valueType accepts the values: "literal", "field"
func (f *generateCSV) buildConfig(config map[string]interface{}) (generateCSVConfig, error) {
	c := generateCSVConfig{}
	c.header = config["header"].(string)

	headerValsRaw := config["headerValue"].([]interface{})
	headerVals := make([]string, 0, len(headerValsRaw))
	for idx := range headerValsRaw {
		headerVals = append(headerVals, headerValsRaw[idx].(string))
	}
	c.headerValues = headerVals

	//c.dataFields = config["dataFields"].([]string)
	c.delimiter = config["delimiter"].(string)
	c.filename = config["filename"].(string)
	return c, nil
}

func (f *generateCSV) Execute(in dataflow.InputReader, out dataflow.OutputWriter, config map[string]interface{}) error {
	defer out.Close()

	//Parse/read config options
	splitOpts, err := f.buildConfig(config)
	if err != nil {
		panic("Error generating a csv")
	}

	//Open the data stream
	reader := in.PortReader(dataflow.DEFAULT_INPUT_PORT_NAME)
	err = reader.Open()
	if err != nil {
		panic("Unable to read record from input")
	}

	//csvfile, err := os.Create("/Users/matthewpaden/go/src/TestingGo/Output/test.csv")
	//if err != nil {
	//	panic("Something went wrong writing the file")
	//}
	csvfile, err := out.NewFileWriter(dataflow.DEFAULT_OUTPUT_PORT_NAME, splitOpts.filename)
	defer csvfile.Close()
	csvwriter := csv.NewWriter(csvfile.Writer())

	//Set the delimeter
	if len(splitOpts.delimiter) > 1 {
		panic("CSV delimiter can only be a single character")
	}

	if splitOpts.delimiter != "" {
		csvwriter.Comma = ([]rune(splitOpts.delimiter))[0]
	}

	//creates the header based on what the is in the headervalues
	if splitOpts.header == "true" {
		csvwriter.Write(splitOpts.headerValues)
	}

	//Loop through all data
	for reader.HasNext() {
		rec, err := reader.Next()
		if err != nil {
			panic("Failed to read record")
		}

		//We should perform a check here to make sure we are getting records instead of files
		recData, err := rec.GetAsRecord()
		if err != nil {
			panic("Failed to read record as a map")
		}

		/* START - The actual work */
		//Loops through the records and gets each field if it is in the headervalue
		//and then joins with what is in the delimiter config.
		newRow := make([]string, 0, len(splitOpts.headerValues))

		for c := range splitOpts.headerValues {
			val, ok := recData.Get(splitOpts.headerValues[c])
			if !ok {
				val = ""
			}

			newRow = append(newRow, fmt.Sprintf("%v", val))
		}

		csvwriter.Write(newRow)
	}

	csvwriter.Flush()

	csvfile.Close()
	return nil
}
