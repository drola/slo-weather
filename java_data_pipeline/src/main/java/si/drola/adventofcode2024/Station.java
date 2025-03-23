package si.drola.adventofcode2024;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize()
public record Station(String meteosiId, String title, String longTitle,
                      Coordinates coordinates) implements Comparable<Station> {
    @Override
    public int compareTo(Station o) {
        return this.meteosiId.compareTo(o.meteosiId);
    }
}
