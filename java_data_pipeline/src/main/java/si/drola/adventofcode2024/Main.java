package si.drola.adventofcode2024;

import com.ctc.wstx.sax.WstxSAXParserFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.nio.file.Files;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static java.time.temporal.ChronoUnit.SECONDS;


class MyHandler extends DefaultHandler {
    public ArrayList<MetDataPoint> dataPoints;
    private StringBuffer buffer;
    private HashMap<String, String> data;

    // Date example: 15.03.2025 18:30 UTC
    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy H:mm z");

    public MyHandler() {
        dataPoints = new ArrayList<>();
        buffer = new StringBuffer();
        data = new HashMap<>();
    }

    public void startElement(String uri, String localName, String qName, Attributes attributes) {
        buffer.delete(0, buffer.length());
        if (qName.equals("metData")) {
            data.clear();
        }
    }

    public void endElement(String uri, String localName, String qName) {
//        System.out.println("End Element: " + qName);
        if (qName.equals("metData")) {
            Instant validEndParsed = null;
            if (data.containsKey("validEnd")) {
                try {
                    validEndParsed = Instant.from(dateTimeFormatter.parse(data.get("validEnd")));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }


            dataPoints.add(new MetDataPoint(
                    data.get("domain_meteosiId").replaceAll("_+$", ""),
                    validEndParsed,
                    Float.parseFloat(data.getOrDefault("rr_val", "0"))
            ));
        } else if (qName.equals("validStart") || qName.equals("validEnd") || qName.equals("domain_meteosiId") || qName.equals("rr_val")) {
            if (!buffer.isEmpty()) {
                data.put(qName, buffer.toString());
            }
        }
    }

    public void characters(char[] ch, int start, int length) {
        this.buffer.append(ch, start, length);
    }
}

public class Main {

    public static Stream<String> readLines(File file) {
        try {
            InputStream stream = new FileInputStream(file);
            if (file.getName().endsWith(".gz")) {
                stream = new GZIPInputStream(stream);
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
            return reader.lines();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error reading file %s".formatted(file.getName()));
        }
    }

    private static ParsedFile parseFile(File file) {
        System.out.println("Parsing file " + file.getName());
        ObjectMapper mapper = new ObjectMapper();
        MyHandler handler = new MyHandler();
        SAXParserFactory factory = WstxSAXParserFactory.newInstance(); //SAXParserFactory.newInstance();
        factory.setNamespaceAware(false);
        factory.setValidating(false);
        TypeReference<HashMap<String, String>> typeRef
                = new TypeReference<HashMap<String, String>>() {
        };
        try {
            SAXParser parser = factory.newSAXParser();
            readLines(file).forEach(line -> {
                Map<String, String> json = null;
                try {
                    json = mapper.readValue(line, typeRef);
                    String xml = json.get("xml");
                    parser.parse(new ByteArrayInputStream(xml.getBytes()), handler);
                } catch (SAXException | IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ParsedFile(file.getName(), handler.dataPoints);
    }

    public static List<MetDataPoint> parallel(File[] files) {
        long start = System.currentTimeMillis();
        var dataPoints = Arrays.stream(files).parallel().map(Main::parseFile).sorted().collect(
                ArrayList<MetDataPoint>::new,
                (list, parsedFile) -> list.addAll(parsedFile.dataPoints()),
                ArrayList::addAll
        );
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println(timeElapsed / 1000);

        return dataPoints;
    }

    public static void sequential(File[] files) throws IOException, ParserConfigurationException, SAXException {
        long start = System.currentTimeMillis();

        for (File file : files) {
            System.out.println(file.getAbsoluteFile());
            var data = parseFile(file);
            System.out.println(data.dataPoints().size());
        }

//        long countPrecipitation = handler.dataPoints.values().stream().filter(dataPoint -> dataPoint.precipitationSum10min() > 0).count();
//        long distinctIntervalsAll = handler.dataPoints.values().stream().map(MetDataPoint::intervalStart).distinct().count();
//        long distinctIntervalsRainy = handler.dataPoints.values().stream().filter(dataPoint -> dataPoint.precipitationSum10min() > 0).map(MetDataPoint::intervalStart).distinct().count();
//        long distinctValues = handler.dataPoints.values().stream().map(MetDataPoint::precipitationSum10min).distinct().count();
//        System.out.printf("Total count: %s%n", handler.dataPoints.size());
//        System.out.printf("Precipitation: %s%n", countPrecipitation);
//        System.out.printf("Distinct intervals (all): %s%n", distinctIntervalsAll);
//        System.out.printf("Distinct intervals (rainy): %s%n", distinctIntervalsRainy);
//        System.out.printf("Distinct values: %s%n", distinctValues);
//
//
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println(timeElapsed / 1000);
    }

    public static List<Station> readStations(File dataDir) {
        File[] files = Objects.requireNonNullElse(
                dataDir.listFiles(
                        (dir, name) -> name.startsWith("stations_") &&
                                (name.endsWith(".json") || name.endsWith(".json.gz"))),
                new File[0]);

        Arrays.sort(files);
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        return readLines(files[files.length - 1]).map((line) -> {
            try {
                return mapper.readValue(line, Station.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).toList();
    }

    public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException {

        File dataDir = new File("/home/drola/work/slo-weather/data");
        var stations = readStations(dataDir).toArray(new Station[0]);
        Arrays.sort(stations);


        File[] files = Objects.requireNonNullElse(
                dataDir.listFiles(
                        (dir, name) -> name.startsWith("meteo_data_archive_") &&
                                (name.endsWith(".json") || name.endsWith(".json.gz"))),
                new File[0]);

        Arrays.sort(files);

//        sequential(files);


        var dataPoints = parallel(files);
        var dataPointsGroupedByIntervalEnd = dataPoints.stream().collect(
                Collectors.groupingBy(MetDataPoint::intervalEnd)
        );


        var distinctDates = dataPointsGroupedByIntervalEnd.keySet().stream().sorted().toList();

        Instant previousDate = null;
        var deltas = new HashMap<Long, Long>();
        for (Instant date : distinctDates) {
            if (previousDate != null) {
                var delta = SECONDS.between(previousDate, date);
                deltas.put(delta, deltas.getOrDefault(delta, 0L) + 1);
            }
            previousDate = date;
        }
        deltas.forEach((k, v) -> System.out.printf("Delta: %s, Count: %s%n", k, v));

        System.out.printf("Dates count: %s%n", distinctDates.size());

        var firstDate = distinctDates.get(0);
        var lastDate = distinctDates.get(distinctDates.size() - 1);
        var intervalsCount = Math.round((float) SECONDS.between(firstDate, lastDate) / 600);
        var matrix = new float[intervalsCount][128];
        for (int i = 0; i < intervalsCount; i++) {
            var date = firstDate.plusSeconds(i * 600L);
            if (!dataPointsGroupedByIntervalEnd.containsKey(date)) {
                continue;
            }
            var valuesForDate = dataPointsGroupedByIntervalEnd.get(date);
            for (int j = 0; j < stations.length; j++) {
                var stationName = stations[j].meteosiId();
                matrix[i][j] = valuesForDate.stream().filter(v -> Objects.equals(v.stationArsoCode(), stationName)).map(MetDataPoint::precipitationSum10min).findFirst().orElse(0f);
            }
        }

        var outputFile = "/home/drola/work/slo-weather/java_data_pipeline/out.data";
        var file = new File(outputFile);
        var outputStream = Files.newOutputStream(file.toPath());
        var bufferedOutputStream = new BufferedOutputStream(outputStream);
        var dataOutputStream = new DataOutputStream(bufferedOutputStream);
        var nonZeroCount = 0;
        for (int i = 0; i < intervalsCount; i++) {
            for (int j = 0; j < 128; j++) {
                if (matrix[i][j] > 0) {
                    nonZeroCount++;
                }
                dataOutputStream.writeFloat(matrix[i][j]);
            }
        }
        System.out.printf("Non-zero count: %s%n", nonZeroCount);
        dataOutputStream.close();


        ExportedDataShape dataShape = new ExportedDataShape(
                firstDate,
                128,
                intervalsCount,
                stations
        );
        // Serialize the dataShape object to a JSON file
        ObjectMapper mapper = JsonMapper.builder()
                .findAndAddModules() // Important for java.time classes
                .build();
        mapper.writeValue(new File("/home/drola/work/slo-weather/java_data_pipeline/out.json"), dataShape);
    }
}