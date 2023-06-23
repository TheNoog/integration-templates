package main

import (
    "encoding/csv"
    "encoding/json"
    "fmt"
    "log"
    "os"
    "strconv"
	"io/ioutil"
)

func decodeJson(m map[string]interface{}) []string {
    values := make([]string, 0, len(m))
    for _, v := range m {
        switch vv := v.(type) {
        case map[string]interface{}:
            for _, value := range decodeJson(vv) {
                values = append(values, value)
            }
        case string:
            values = append(values, vv)
        case float64:
            values = append(values, strconv.FormatFloat(vv, 'f', -1, 64))
        case []interface{}:
            // Arrays aren't currently handled, since you haven't indicated that we should
            // and it's non-trivial to do so.
        case bool:
            values = append(values, strconv.FormatBool(vv))
        case nil:
            values = append(values, "nil")
        }
    }
    return values
}


func main() {

	fileContent, err1 := os.Open("data.json")

	if err1 != nil {
	   log.Fatal(err1)
	   return
	}
 
	fmt.Println("The File is opened successfully...")
	defer fileContent.Close()
	byteResult, _ := ioutil.ReadAll(fileContent)
	var res map[string]interface{}
	json.Unmarshal([]byte(byteResult), &res)
	fmt.Println(res["results"])




    var d interface{}  // should d be replaced with res ?????
    // err := json.Unmarshal(exampleJSON, &d)
    err := json.Unmarshal(byteResult, &res)

    if err != nil {
        log.Fatal("Failed to unmarshal")
    }
    values := decodeJson(d.(map[string]interface{}))
    fmt.Println(values)

    f, err := os.Create("outputfile.csv")
    if err != nil {
        log.Fatal("Failed to create outputfile.csv")
    }
    defer f.Close()
    w := csv.NewWriter(f)
    if err := w.Write(values); err != nil {
        log.Fatal("Failed to write to file")
    }
    w.Flush()
    if err := w.Error(); err != nil {
        log.Fatal("Failed to flush outputfile.csv")
    }
}

