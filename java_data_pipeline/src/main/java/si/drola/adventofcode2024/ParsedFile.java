package si.drola.adventofcode2024;

import java.util.List;

public record ParsedFile(
        String filename,
        List<MetDataPoint> dataPoints) implements Comparable<ParsedFile> {

    @Override
    public int compareTo(ParsedFile o) {
        return this.filename.compareTo(o.filename);
    }
}
