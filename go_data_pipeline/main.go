package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"time"
)

func get_input_files_list(data_dir string) []string {
	valid_patterns := []string{"meteo_data_archive_*.json", "meteo_data_archive_*.json.gz"}
	files := []string{}
	err := filepath.WalkDir(data_dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			fmt.Println("Error accessing path", path)
			return err
		}

		for _, pattern := range valid_patterns {
			is_match, err := filepath.Match(pattern, d.Name())
			if err != nil {
				fmt.Println("Error matching pattern", pattern)
			}
			if is_match {
				files = append(files, path)
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil
	}
	return files
}

type MeteoDataJsonLine struct {
	Xml string `json:"xml"`
}

type MeteoDataXml struct {
	StationArsoCode         string       `xml:"domain_meteosiId"`
	Sunrise                 meteoXmlTime `xml:"sunrise"`
	Sunset                  meteoXmlTime `xml:"sunset"`
	IntervalStart           meteoXmlTime `xml:"validStart"`
	IntervalEnd             meteoXmlTime `xml:"validEnd"`
	TemperatureDewPoint     float64      `xml:"td"`
	TemperatureAirAvg       float64      `xml:"tavg"`
	TemperatureAirMax       float64      `xml:"tx"`
	TemperatureAirMin       float64      `xml:"tn"`
	HumidityRelativeAvg     float64      `xml:"rhavg"`
	WindDirectionAvg        float64      `xml:"ddavg_val"`
	WindDirectionMaxGust    float64      `xml:"ddmax_val"`
	WindSpeedAvg            float64      `xml:"ffavg_val"`
	WindSpeedMax            float64      `xml:"ffmax_val"`
	PressureMeanSeaLevelAvg float64      `xml:"mslavg"`
	PressureSurfaceLevelAvg float64      `xml:"pavg"`
	PrecipitationSum10min   float64      `xml:"rr_val"`
	PrecipitationSum1h      float64      `xml:"tp_1h_acc"`
	PrecipitationSum24h     float64      `xml:"tp_24h_acc"`
	SnowCoverHeight         float64      `xml:"snow"`
	SunRadiationGlobalAvg   float64      `xml:"gSunRadavg"`
	SunRadiationDiffuseAvg  float64      `xml:"diffSunRadavg"`
	Visibility              float64      `xml:"vis_val"`
}

type MeteoDataXmlDocument struct {
	MetData []MeteoDataXml `xml:"metData"`
}

type meteoXmlTime struct {
	time.Time
}

func (c *meteoXmlTime) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	const format = "2.1.2006 15:04 MST" // https://pkg.go.dev/time#Layout
	var v string
	err := d.DecodeElement(&v, &start)
	if err != nil {
		return err
	}
	parse, err := time.Parse(format, v)
	if err != nil {
		return err
	}
	*c = meteoXmlTime{parse}
	return nil
}

//func (c *MeteoDataXml) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
//	for {
//		tok, err := d.Token()
//		if err != nil {
//			return err
//		}
//		switch t := tok.(type) {
//		case xml.EndElement:
//			if t.Name.Local == "metData" {
//				return nil
//			}
//		}
//	}
//}

func processFile(inputFile string) []MeteoDataXml {
	allData := make([]MeteoDataXml, 0)

	fmt.Println("Processing file", inputFile)
	var reader io.Reader
	fileReader, err := os.Open(inputFile)
	if err != nil {
		fmt.Println("Error opening file", inputFile)
		return nil
	}
	defer func(fileReader *os.File) {
		err := fileReader.Close()
		if err != nil {
			fmt.Println("Error closing file", inputFile)
		}
	}(fileReader)

	if filepath.Ext(inputFile) == ".gz" {
		gzipReader, err := gzip.NewReader(fileReader)
		if err != nil {
			fmt.Println("Error creating gzip reader for file", inputFile)
			return nil
		}
		defer func(gzipReader *gzip.Reader) {
			err := gzipReader.Close()
			if err != nil {
				fmt.Println("Error closing gzip reader for file", inputFile)
			}
		}(gzipReader)
		reader = gzipReader
	} else {
		reader = fileReader
	}

	scanner := bufio.NewScanner(reader)
	println("Scanning file", inputFile)
	buf := make([]byte, 0, 1024*1024*10)
	scanner.Buffer(buf, 1024*1024*10)
	for scanner.Scan() {
		obj := MeteoDataJsonLine{}
		err := json.Unmarshal(scanner.Bytes(), &obj)
		if err != nil {
			println(err.Error())
			panic(err)
		}

		//tokenCount := 0
		//decoder := xml.NewDecoder(strings.NewReader(obj.Xml))
		//for _, err := decoder.RawToken(); err == nil; _, err = decoder.RawToken() { // <-- here
		//	tokenCount++
		//}

		//println("Token count", tokenCount)
		//
		xmlObjs := MeteoDataXmlDocument{}
		err = xml.Unmarshal([]byte(obj.Xml), &xmlObjs)
		allData = append(allData, xmlObjs.MetData...)
		if err != nil {
			log.Fatalf("xml error: %v", err)
			return nil
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("scan file error: %v", err)
		return nil
	}

	return allData
}

func main() {
	dataDir := "/home/drola/work/slo-weather/data"

	allData := make(map[string]MeteoDataXml)

	fileCount := 0
	for _, inputFile := range get_input_files_list(dataDir) {
		fmt.Println("Processing file", inputFile)
		dataFromFile := processFile(inputFile)
		for _, data := range dataFromFile {
			allData[fmt.Sprintf("%s-%s", data.StationArsoCode, data.IntervalStart)] = data
		}
		println("Count of allData", len(allData))
		fileCount++
		if len(allData) > 50000 || fileCount >= 3 {
			break
		}
	}
}
