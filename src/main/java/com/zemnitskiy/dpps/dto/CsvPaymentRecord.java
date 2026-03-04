package com.zemnitskiy.dpps.dto;

import com.opencsv.bean.CsvBindByName;
import lombok.Data;

/** Raw CSV row mapped by OpenCSV annotations. Validated and converted to Payment in CsvParsingService. */
@Data
public class CsvPaymentRecord {

    @CsvBindByName
    private String id;

    @CsvBindByName(column = "datetime")
    private String dateTime;

    @CsvBindByName
    private String sender;

    @CsvBindByName
    private String receiver;

    @CsvBindByName(column = "amount")
    private String amount;

    @CsvBindByName(column = "value")
    private String value;

    /** Returns whichever of "amount" or "value" column was present in the CSV. */
    public String getAmountValue() {
        if (amount != null && !amount.isBlank()) return amount;
        return value;
    }
}
