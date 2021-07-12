package builtin

import (
	"bitbucket.org/primelogic_io/bitlantern/service/dataflow"
	"bufio"
	"fmt"
	_ "github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
	"strconv"
	"strings"
	"time"
)

func init() {
	//Register function with default builtin.FunctionProvider
	DefaultInstance().RegisterFunction(
		Function{
			FunctionSpec: dataflow.FunctionSpec{
				Key:           "parseFixedLength",
				Name:          "Parses Fixed Length Text",
				Description:   "Parses a fixed length text file and outputs the records",
				Category:      "Data",
				ExecutionMode: "sync",
				InputPorts:    nil,
				OutputPorts:   nil,
			},
			NewFunction: func() dataflow.Function {
				return &(parseFixedLength{})
			},
		})
}

type parseFixedLength struct {
	config parseFixedLengthConfig
}

type parseFixedLengthConfig struct {
	hasHeaderRow bool
	columns      []parseFixedLengthColumn
}

type parseFixedLengthColumn struct {
	start     int
	length    int
	datatype  string //number, string,
	format    string //String describing the format of the string to be converted (currently only for date)
	fieldName string
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
func (f *parseFixedLength) buildConfig(config map[string]interface{}) (parseFixedLengthConfig, error) {
	c := parseFixedLengthConfig{}

	columns := config["columns"].([]interface{})
	c.columns = make([]parseFixedLengthColumn, len(columns))

	for i := 0; i < len(columns); i++ {
		curRuleMap := columns[i].(map[string]interface{})
		newRule := parseFixedLengthColumn{}

		//FIXME: Use a better type conversion here
		newRule.start = int(curRuleMap["start"].(float64))
		newRule.length = int(curRuleMap["length"].(float64))
		newRule.datatype = curRuleMap["datatype"].(string)
		newRule.format, _ = curRuleMap["format"].(string) //Ignore if can't convert, probably means its missing
		newRule.fieldName = curRuleMap["fieldName"].(string)

		c.columns[i] = newRule
	}

	c.hasHeaderRow = config["hasHeaderRow"].(bool)

	return c, nil
}

func (f *parseFixedLength) Execute(in dataflow.InputReader, out dataflow.OutputWriter, config map[string]interface{}) error {
	defer out.Close()

	//Parse/read config options
	parsedConfig, err := f.buildConfig(config)
	if err != nil {
		panic("Error generating a csv")
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

		f.parseFile(curFile, out)
	}

	return nil
}

func (f *parseFixedLength) parseFile(file dataflow.File, out dataflow.OutputWriter) {
	fileReader := file.Reader()

	scan := bufio.NewScanner(fileReader)

	if f.config.hasHeaderRow {
		//throw away first row. we dont even need it for the column names, since we got them in the config
		scan.Scan()
	}

	for scan.Scan() {
		line := scan.Text()
		rec, err := f.parseLine(line)

		if err != nil {
			panic(err)
		}

		out.WriteRecord(dataflow.DEFAULT_OUTPUT_PORT_NAME, &rec)
	}
}

func (f *parseFixedLength) parseLine(line string) (dataflow.Record, error) {
	rec := dataflow.Record{}

	for idx := range f.config.columns {
		col := f.config.columns[idx]
		valStr := line[col.start:(col.start + col.length)]

		if col.datatype == "string" {
			//Already done :-)
			rec.Set(col.fieldName, strings.TrimSpace(valStr))
		} else if col.datatype == "integer" {
			valInt, err := strconv.ParseInt(strings.TrimSpace(valStr), 10, 64)
			if err != nil {
				panic(err)
			}
			rec.Set(col.fieldName, valInt)
		} else if col.datatype == "decimal" {
			valFloat, err := strconv.ParseFloat(strings.TrimSpace(valStr), 64)
			if err != nil {
				panic(err)
			}
			rec.Set(col.fieldName, valFloat)
		} else if col.datatype == "date" {
			valDate, err := time.Parse(col.format, strings.TrimSpace(valStr))
			if err != nil {
				panic(err)
			}
			rec.Set(col.fieldName, valDate)
		} else {
			panic(fmt.Sprintf("Unsupported dataype %v for column %v", col.datatype, col.fieldName))
		}
	}

	return rec, nil
}
