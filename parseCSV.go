package builtin

import (
	"bitbucket.org/primelogic_io/bitlantern/service/dataflow"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

func init() {
	//Register function with default builtin.FunctionProvider
	DefaultInstance().RegisterFunction(
		Function{
			FunctionSpec: dataflow.FunctionSpec{
				Key:           "parseCSV",
				Name:          "Parses Delimited Text File",
				Description:   "Parses delimited text files and outputs the records",
				Category:      "File",
				ExecutionMode: "sync",
				InputPorts:    nil,
				OutputPorts:   nil,
			},
			NewFunction: func() dataflow.Function {
				return &(parseCSV{})
			},
		})
}

type parseCSV struct {
	config         parseCSVConfig
	headerNames    []string
	headerIndexMap map[string]int
}

type parseCSVConfig struct {
	// hasHeaderRow indicates if the first row in the CSV is the header.
	hasHeaderRow bool

	// useHeaderColumnNamesAsFieldNames indicates whether the column names in the header row should be used as the field names when parsing the data.
	useHeaderColumnNamesAsFieldNames bool

	// ignoreUnmappedColumns indicates whether columns not specific in the config should be ignored or parsed using the default config.
	ignoreUnmappedColumns bool

	// delimiter specifies the delimiter, if not a comma
	delimiter string

	// columns specify the parsing rules for each column. If ignoreUnmappedColumns is true, then only the columns specified here will be included in the output.
	columns []parseCSVColumn
}

type parseCSVColumn struct {
	//The index of the column in the CSV file, either this or the column name must be specified, and if column name the file must have a header
	index int

	// columnName is the csv header/column name for this column. This config option is only valid if the CSV file has a header row.
	columnName string

	// datatype is the type of data in each row in the field/column. This will
	datatype string //number, string, date
	format   string //String describing the format of the string to be converted (currently only for date)

	// fieldName to use for the output
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
func (f *parseCSV) buildConfig(config map[string]interface{}) (parseCSVConfig, error) {
	c := parseCSVConfig{}

	columns := config["columns"].([]interface{})
	c.columns = make([]parseCSVColumn, len(columns))

	for i := 0; i < len(columns); i++ {
		curRuleMap := columns[i].(map[string]interface{})
		newRule := parseCSVColumn{}

		//FIXME: Use a better type conversion here
		newRule.columnName = curRuleMap["columnName"].(string)
		newRule.datatype = curRuleMap["datatype"].(string)
		newRule.format, _ = curRuleMap["format"].(string) //Ignore if can't convert, probably means its missing
		newRule.fieldName = curRuleMap["fieldName"].(string)

		c.columns[i] = newRule
	}

	c.hasHeaderRow = config["hasHeaderRow"].(bool)
	c.useHeaderColumnNamesAsFieldNames = config["useHeaderColumnNamesAsFieldNames"].(bool)
	c.delimiter, _ = config["delimiter"].(string)

	return c, nil
}

func (f *parseCSV) Execute(in dataflow.InputReader, out dataflow.OutputWriter, config map[string]interface{}) error {
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

func (f *parseCSV) parseFile(file dataflow.File, out dataflow.OutputWriter) {
	csv := csv.NewReader(file.Reader())

	if len(f.config.delimiter) == 1 {
		csv.Comma = ([]rune(f.config.delimiter))[0]
	}

	if f.config.hasHeaderRow {
		f.headerIndexMap = make(map[string]int)
		header, err := csv.Read()
		if err != nil {
			panic(err)
		}

		f.headerNames = header

		for idx := range header {
			f.headerIndexMap[header[idx]] = idx
		}
	}

	//Read each record
	curRec, err := csv.Read()
	if err != nil {
		panic(err)
	}

	for curRec != nil {
		recObj, err := f.parseLine(curRec)
		if err != nil {
			panic(err)
		}

		out.WriteRecord(dataflow.DEFAULT_OUTPUT_PORT_NAME, &recObj)

		//Read next line
		curRec, err = csv.Read()
		if err != nil {
			if err == io.EOF {
				//EOF
				break
			} else {
				panic(err)
			}
		}
	}
}

func (f *parseCSV) getColumnConfigByHeaderName(headerName string) *parseCSVColumn {
	for i := range f.config.columns {
		if f.config.columns[i].columnName == headerName {
			return &(f.config.columns[i])
		}
	}

	return nil
}

func (f *parseCSV) getColumnConfigByIndex(index int) *parseCSVColumn {
	for i := range f.config.columns {
		if f.config.columns[i].index == index {
			return &(f.config.columns[i])
		}
	}

	return nil
}

func (f *parseCSV) parseLine(record []string) (dataflow.Record, error) {
	rec := dataflow.Record{}

	for recIdx := range record {
		curRec := record[recIdx]

		//Attempt to find the column config
		var colConfig *parseCSVColumn
		if f.config.hasHeaderRow {
			//Attempt to find by name
			colConfig = f.getColumnConfigByHeaderName(f.headerNames[recIdx])
		}

		if colConfig == nil {
			colConfig = f.getColumnConfigByIndex(recIdx)
		}

		if colConfig == nil && f.config.ignoreUnmappedColumns {
			//Skip this unmapped column
			continue
		}

		if colConfig == nil && !f.config.hasHeaderRow {
			//Then we don't have enough information to parse this column
			panic("Unable to output CSV field. There must be a header row OR a column config for each column, if ignoreUnmappedColumns is false")
		}

		//Read the record value
		if colConfig.datatype == "string" {
			//Already done :-)
			rec.Set(colConfig.fieldName, curRec)
		} else if colConfig.datatype == "integer" {
			valInt, err := strconv.ParseInt(curRec, 10, 64)
			if err != nil {
				panic(err)
			}
			rec.Set(colConfig.fieldName, valInt)
		} else if colConfig.datatype == "decimal" {
			valFloat, err := strconv.ParseFloat(curRec, 64)
			if err != nil {
				panic(err)
			}
			rec.Set(colConfig.fieldName, valFloat)
		} else if colConfig.datatype == "date" {
			valDate, err := time.Parse(colConfig.format, strings.TrimSpace(curRec))
			if err != nil {
				panic(err)
			}
			rec.Set(colConfig.fieldName, valDate)
		} else {
			panic(fmt.Sprintf("Unsupported dataype %v for column %v", colConfig.datatype, colConfig.fieldName))
		}
	}

	return rec, nil
}
