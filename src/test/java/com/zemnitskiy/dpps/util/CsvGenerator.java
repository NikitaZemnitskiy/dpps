package com.zemnitskiy.dpps.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates a CSV file with random payment records for load testing.
 * <p>
 * Usage via Maven (no extra dependencies needed)
 * Or directly:
 * <pre>
 *   java -cp target/classes com.zemnitskiy.dpps.util.CsvGenerator [options]
 * </pre>
 * <p>
 * Options:
 * <ul>
 *   <li>{@code -n <count>}      — number of rows (default: 1000000)</li>
 *   <li>{@code -o <file>}       — output file (default: payments-generated.csv)</li>
 *   <li>{@code --banks <count>} — number of banks (default: 10)</li>
 *   <li>{@code --min <value>}   — min payment amount (default: 1.0)</li>
 *   <li>{@code --max <value>}   — max payment amount (default: 10000.0)</li>
 *   <li>{@code --start <date>}  — start date yyyy-MM-dd (default: 2026-01-01)</li>
 *   <li>{@code --end <date>}    — end date yyyy-MM-dd (default: 2026-12-31)</li>
 * </ul>
 */
public class CsvGenerator {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    public static void main(String[] args) throws IOException {
        int rows = intArg(args, "-n", 1_000_000);
        String output = stringArg(args, "-o", "payments-generated.csv");
        int bankCount = intArg(args, "--banks", 10);
        double minValue = doubleArg(args, "--min", 1.0);
        double maxValue = doubleArg(args, "--max", 10_000.0);
        LocalDateTime start = LocalDateTime.parse(stringArg(args, "--start", "2026-01-01") + "T00:00:00");
        LocalDateTime end = LocalDateTime.parse(stringArg(args, "--end", "2026-12-31") + "T23:59:59");

        long deltaSeconds = java.time.Duration.between(start, end).getSeconds();
        String[] banks = generateBankNames(bankCount);

        System.out.printf("Generating %,d rows -> %s%n", rows, output);
        System.out.printf("  Banks: %d  |  Values: %.1f - %.1f%n", bankCount, minValue, maxValue);
        System.out.printf("  Date range: %s to %s%n", start.toLocalDate(), end.toLocalDate());

        long t0 = System.currentTimeMillis();

        try (BufferedWriter writer = Files.newBufferedWriter(Path.of(output))) {
            writer.write("DateTime,Sender,Receiver,Amount,ID");
            writer.newLine();

            ThreadLocalRandom rng = ThreadLocalRandom.current();

            for (int i = 1; i <= rows; i++) {
                LocalDateTime dt = start.plusSeconds(rng.nextLong(deltaSeconds + 1));
                int senderIdx = rng.nextInt(bankCount);
                int receiverIdx = rng.nextInt(bankCount - 1);
                if (receiverIdx >= senderIdx) receiverIdx++;
                double amount = Math.round(rng.nextDouble(minValue, maxValue) * 100.0) / 100.0;

                writer.write(dt.format(FMT));
                writer.write(',');
                writer.write(banks[senderIdx]);
                writer.write(',');
                writer.write(banks[receiverIdx]);
                writer.write(',');
                writer.write(String.valueOf(amount));
                writer.write(',');
                writer.write(String.valueOf(i));
                writer.newLine();

                if (i % 500_000 == 0) {
                    long elapsed = System.currentTimeMillis() - t0;
                    System.out.printf("  ... %,d rows (%.1fs)%n", i, elapsed / 1000.0);
                }
            }
        }

        long elapsed = System.currentTimeMillis() - t0;
        long sizeBytes = Files.size(Path.of(output));
        System.out.printf("Done: %,d rows, %.1f MB, %.1fs%n", rows, sizeBytes / (1024.0 * 1024.0), elapsed / 1000.0);
    }

    private static String[] generateBankNames(int count) {
        String[] banks = new String[count];
        for (int i = 0; i < count; i++) {
            banks[i] = i < 26 ? "Bank-" + (char) ('A' + i) : "Bank-" + (i + 1);
        }
        return banks;
    }

    private static String stringArg(String[] args, String key, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(key)) return args[i + 1];
        }
        return defaultValue;
    }

    private static int intArg(String[] args, String key, int defaultValue) {
        String val = stringArg(args, key, null);
        return val != null ? Integer.parseInt(val) : defaultValue;
    }

    private static double doubleArg(String[] args, String key, double defaultValue) {
        String val = stringArg(args, key, null);
        return val != null ? Double.parseDouble(val) : defaultValue;
    }
}
