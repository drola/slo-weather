package si.drola.adventofcode2024;

import java.time.Instant;

public record ExportedDataShape(
        Instant firstIntervalEnd,
        long columns,
        long rows,
        Station[] stations
) {
}
