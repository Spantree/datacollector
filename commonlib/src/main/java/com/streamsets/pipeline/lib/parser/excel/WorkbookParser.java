/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.streamsets.pipeline.lib.parser.excel;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.ProtoConfigurableEntity.Context;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.ExcelHeader;
import com.streamsets.pipeline.lib.parser.AbstractDataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class WorkbookParser extends AbstractDataParser {

  private final WorkbookParserSettings settings;
  private final Context context;
  private final Workbook workbook;
  private final ListIterator<Row> rowIterator;
  private String offset;
  private boolean eof;

  private HashMap<String, List<Field>> headers;

  public WorkbookParser(WorkbookParserSettings settings,
                        Context context,
                        Workbook workbook,
                        String offsetId
  ) throws DataParserException {
    this.settings = requireNonNull(settings);
    this.context = requireNonNull(context);
    this.workbook = requireNonNull(workbook);
    this.rowIterator = iterate(this.workbook);
    this.offset = requireNonNull(offsetId);

    if (!rowIterator.hasNext()) {
      throw new DataParserException(Errors.EXCEL_PARSER_04);
    }

    switch (settings.getHeader()) {
      case WITH_HEADER:
        rowIterator.next();
        headers = new HashMap<>();
        for (Sheet sheet : workbook) {
          List<Field> sheetHeaders = new ArrayList<>();
          Row headerRow = sheet.getRow(0);
          for (Cell cell : headerRow) {
            sheetHeaders.add(Cells.parseCell(cell));
          }
          headers.put(sheet.getSheetName(), sheetHeaders);
        }

        break;
      case IGNORE_HEADER:
        Row ignored = rowIterator.next();
        headers = new HashMap<>();
        break;
      case NO_HEADER:
        headers = new HashMap<>();
        break;
    }

    Offsets.parse(offsetId).ifPresent(offset -> {
      String startSheetName = offset.getSheetName();
      int startRowNum = offset.getRowNum();

      while (rowIterator.hasNext()) {
        Row row = rowIterator.next();
        int rowNum = row.getRowNum();
        String sheetName = row.getSheet().getSheetName();
        if (startSheetName.equals(sheetName) && rowNum == startRowNum) {
          if (rowIterator.hasPrevious()) {
            rowIterator.previous();
          }
          break;
        }
      }
    });
  }

  private static ListIterator<Row> iterate(Workbook workbook) {
    return stream(workbook).flatMap(WorkbookParser::stream).collect(toList()).listIterator();
  }

  private static <T> Stream<T> stream(Iterable<T> it) {
    return StreamSupport.stream(it.spliterator(), false);
  }

  @Override
  public Record parse() throws DataParserException {
    if (!rowIterator.hasNext()) {
      eof = true;
      return null;
    }
    Row currentRow = rowIterator.next();
    if ((settings.getHeader() == ExcelHeader.WITH_HEADER || settings.getHeader() == ExcelHeader.IGNORE_HEADER)
        && currentRow.getRowNum() == 0) {
      currentRow = rowIterator.next();
    }
    offset = Offsets.offsetOf(currentRow);
    Record record = context.createRecord(offset);
    LinkedHashMap<String, Field> rowMap = readRow(currentRow);
    record.set(Field.createListMap(rowMap));
    return record;
  }

  @Override
  public String getOffset() {
    return eof ? "-1" : offset;
  }

  @Override
  public void close() throws IOException {
    workbook.close();
  }

  private LinkedHashMap<String, Field> readRow(Row row) throws DataParserException {
    LinkedHashMap<String, Field> output = new LinkedHashMap<>();
    String sheetName = row.getSheet().getSheetName();
    String columnHeader;
    for (int columnNum = 0; columnNum < row.getLastCellNum(); columnNum++) {
      if (headers.isEmpty()) {
        columnHeader = String.valueOf(columnNum);
      }
      else {
        columnHeader = headers.get(sheetName).get(columnNum).getValueAsString();
      }
      Cell cell = row.getCell(columnNum, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
      output.put(columnHeader, Cells.parseCell(cell));
    }
    return output;
  }
}
