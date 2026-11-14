package com.zemnitskiy.dpps.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.SplittableRandom;

/**
 * Generates a CSV file with random payments for load testing.
 * <pre>
 *   ./mvnw -q test-compile exec:java -Dexec.mainClass=com.zemnitskiy.dpps.util.CsvGenerator \
 *       -Dexec.classpathScope=test -Dexec.args="-n 1000000 -o payments-1m.csv"
 * </pre>
 * Options:
 * <ul>
 *   <li>{@code -n <count>}      number of rows (default 1000)</li>
 *   <li>{@code -o <file>}       output file (default payments-generated.csv)</li>
 *   <li>{@code --banks <count>} number of banks (default 10)</li>
 *   <li>{@code --min <amount>}  smallest amount (default 1.00)</li>
 *   <li>{@code --max <amount>}  largest amount (default 10000.00)</li>
 *   <li>{@code --start <date>}  first day, yyyy-MM-dd (default 2026-01-01)</li>
 *   <li>{@code --end <date>}    last day, yyyy-MM-dd (default 2026-12-31)</li>
 *   <li>{@code --seed <long>}   random seed, for reproducible files (default: random)</li>
 * </ul>
 * Amounts are written in plain decimal notation with two decimals, IDs are 1..n.
 */
public final class CsvGenerator {

    private static final DateTimeFormatter DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    private CsvGenerator() {
    }

    public static void main(String[] args) throws IOException {
        int rows = Integer.parseInt(arg(args, "-n", "1000"));
        Path output = Path.of(arg(args, "-o", "payments-generated.csv"));
        int bankCount = Integer.parseInt(arg(args, "--banks", "10"));
        long minCents = cents(arg(args, "--min", "1.00"));
        long maxCents = cents(arg(args, "--max", "10000.00"));
        LocalDateTime start = LocalDate.parse(arg(args, "--start", "2026-01-01")).atStartOfDay();
        LocalDateTime end = LocalDate.parse(arg(args, "--end", "2026-12-31")).atTime(23, 59, 59);
        String seed = arg(args, "--seed", null);
        SplittableRandom random = seed != null ? new SplittableRandom(Long.parseLong(seed)) : new SplittableRandom();

        if (bankCount < 2 || minCents <= 0 || maxCents < minCents || end.isBefore(start)) {
            throw new IllegalArgumentException("Need at least 2 banks, 0 < min <= max and start <= end");
        }
        long seconds = Duration.between(start, end).getSeconds();
        String[] banks = bankNames(bankCount);

        System.out.printf("Generating %,d rows -> %s (%d banks, amounts %s..%s, %s..%s)%n", rows, output, bankCount,
                BigDecimal.valueOf(minCents, 2), BigDecimal.valueOf(maxCents, 2), start.toLocalDate(), end.toLocalDate());
        long started = System.nanoTime();

        try (BufferedWriter writer = Files.newBufferedWriter(output)) {
            writer.write("DateTime,Sender,Receiver,Amount,ID");
            writer.newLine();
            for (int i = 1; i <= rows; i++) {
                int sender = random.nextInt(bankCount);
                int receiver = random.nextInt(bankCount - 1);
                if (receiver >= sender) {
                    receiver++;
                }
                writer.write(start.plusSeconds(random.nextLong(seconds + 1)).format(DATE_TIME));
                writer.write(',');
                writer.write(banks[sender]);
                writer.write(',');
                writer.write(banks[receiver]);
                writer.write(',');
                writer.write(BigDecimal.valueOf(random.nextLong(minCents, maxCents + 1), 2).toPlainString());
                writer.write(',');
                writer.write(Integer.toString(i));
                writer.newLine();
                if (i % 500_000 == 0) {
                    System.out.printf("  ... %,d rows%n", i);
                }
            }
        }

        System.out.printf("Done: %,d rows, %.1f MB in %.1f s%n", rows, Files.size(output) / (1024.0 * 1024.0),
                (System.nanoTime() - started) / 1e9);
    }

    private static String[] bankNames(int count) {
        String[] banks = new String[count];
        for (int i = 0; i < count; i++) {
            banks[i] = i < 26 ? "Bank " + (char) ('A' + i) : "Bank " + (i + 1);
        }
        return banks;
    }

    private static long cents(String amount) {
        return new BigDecimal(amount).setScale(2, RoundingMode.HALF_UP).movePointRight(2).longValueExact();
    }

    private static String arg(String[] args, String name, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }
}
