package builtin

import (
	"bitbucket.org/primelogic_io/bitlantern/service/dataflow"
	"bytes"
	"fmt"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
	"io"
	"text/template"
)

func init() {
	//Register function with default builtin.FunctionProvider
	DefaultInstance().RegisterFunction(
		Function{
			FunctionSpec: dataflow.FunctionSpec{
				Key:           "fileRouter",
				Name:          "File Router",
				Description:   "Routes input files to different outputs based on certain conditions",
				Category:      "File",
				ExecutionMode: "sync",
				InputPorts:    nil,
				OutputPorts:   nil,
			},
			NewFunction: func() dataflow.Function {
				return &(fileRouter{})
			},
		})
}

type fileRouter struct {
}

type fileRouterConfig struct {
	// splits contains the rules for data splitting. The first rule that matches the data will be used.
	rules []fileRouterRule

	// defaultOutputPort specifies the output port into which to send data that doesn't match any other split rule
	defaultOutputPort string
}

type fileRouterRule struct {
	jsCondition string
	outputPort  string
	jsFuncName  string
}

type fileInfo struct {
	Filename string
	//size
	//mime type
}

// buildConfig builds a splitConfig from the passed in map. The map must be in the form:
// {
//		"defaultOutputPort": "ignoredFiles",
//		"rules": [
//			{
//				"jsCondition": "filename.toLowerCase().endsWith(".csv")",
//				"outputPort": "csv"
//			},
//			{
//				"jsCondition": "filename.toLowerCase().endsWith(".txt")",
//				"outputPort": "fixedLength"
//			}
//		]
// }
//
// The available fields for conditions are: filename, size (in bytes), mime type (future)
func (f *fileRouter) buildConfig(config map[string]interface{}) (fileRouterConfig, error) {
	c := fileRouterConfig{}

	c.defaultOutputPort = config["defaultOutputPort"].(string)
	rules := config["rules"].([]interface{})
	c.rules = make([]fileRouterRule, len(rules))

	for i := 0; i < len(rules); i++ {
		curRuleMap := rules[i].(map[string]interface{})
		newRule := fileRouterRule{}

		newRule.outputPort = curRuleMap["outputPort"].(string)
		newRule.jsCondition = curRuleMap["jsCondition"].(string)

		c.rules[i] = newRule
	}

	return c, nil
}

func (f *fileRouter) Execute(in dataflow.InputReader, out dataflow.OutputWriter, config map[string]interface{}) error {
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

	//JS Template - We will wrap the condition express in a function that can be called more easily
	jsFuncTemplate, err := template.New("someName").Parse(`
		var {{.FuncName}} = function(data) {
			if ({{.Condition}}) {
				return true;
			} else {
				return false;
			}
		};
	`)

	vm := otto.New()

	//Parse all the JS rules
	for i := range configOpts.rules {
		curRule := &(configOpts.rules[i])

		var b bytes.Buffer
		templateVars := make(map[string]string)
		templateVars["Condition"] = curRule.jsCondition
		templateVars["FuncName"] = fmt.Sprintf("splitRuleFunc_%d", i)
		jsFuncTemplate.Execute(&b, templateVars)

		fmt.Printf("\n\n%s", b.String())

		_, err := vm.Run(b.String())
		if err != nil {
			panic(err)
		}

		curRule.jsFuncName = templateVars["FuncName"]
	}

	//Loop through all files, routing them based on the conditions
	for reader.HasNext() {
		curEntry, err := reader.Next()
		if err != nil {
			panic(err)
		}

		curFile, err := curEntry.GetAsFile()
		if err != nil {
			panic(err)
		}

		fileReader := curFile.Reader()

		fileInfo := fileInfo{
			Filename: curFile.Filename(),
		}

		// Run the script rules for each record
		outputPort := configOpts.defaultOutputPort
		for i := range configOpts.rules {
			curRule := configOpts.rules[i]

			value, err := vm.Call(curRule.jsFuncName, nil, fileInfo)
			if err != nil {
				panic(err)
			}

			matches, err := value.ToBoolean()
			if err != nil {
				panic(err)
			}

			if matches {
				outputPort = curRule.outputPort
			}
		}

		//Copy file from input to the correct output
		outFile, err := out.NewFileWriter(outputPort, curFile.Filename())
		size, err := io.Copy(outFile.Writer(), fileReader)
		fmt.Printf("Copied file %v with size %v bytes to %v\n", curFile.Filename(), size, outputPort)
	}

	return nil
}
