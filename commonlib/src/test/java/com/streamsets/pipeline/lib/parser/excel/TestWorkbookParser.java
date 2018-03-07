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
 * limitations under the License. See accompanying LICENSE file
 */

package com.streamsets.pipeline.lib.parser.excel;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.ExcelHeader;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;

import static org.junit.Assert.assertEquals;

public class TestWorkbookParser {
  @Rule
  public final ExpectedException exception = ExpectedException.none();

  private InputStream getFile(String path) {
    return ClassLoader.class.getResourceAsStream(path);
  }

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @Test
  public void testParseCorrectlyReturnsCachedValueOfFormula() throws IOException, InvalidFormatException, DataParserException {
    InputStream file = getFile("/TestFileFormulas.xlsx");
    Workbook workbook = WorkbookFactory.create(file);
    WorkbookParserSettings settings = WorkbookParserSettings.builder()
        .withHeader(ExcelHeader.NO_HEADER)
        .build();

    WorkbookParser parser = new WorkbookParser(settings, getContext(), workbook, "Sheet1::0");

    Record recordFirstRow = parser.parse();
    Record recordSecondRow = parser.parse();

    LinkedHashMap<String, Field> firstMap = new LinkedHashMap<>();
    firstMap.put("0", Field.create("Addition"));
    firstMap.put("1", Field.create("Division"));
    firstMap.put("2", Field.create("Neighbor Multiplication"));
    Field expectedFirstRow = Field.createListMap(firstMap);

    LinkedHashMap<String, Field> secondMap = new LinkedHashMap<>();
    secondMap.put("0", Field.create(8.0));
    secondMap.put("1", Field.create(9.0));
    secondMap.put("2", Field.create(72.0));
    Field expectedSecondRow = Field.createListMap(secondMap);

    assertEquals(expectedFirstRow, recordFirstRow.get());
    assertEquals(expectedSecondRow, recordSecondRow.get());
  }

  @Test
  public void testParseThrowsErrorForUnsupportedCellType() throws IOException, InvalidFormatException, DataParserException {
    InputStream file = getFile("/TestErrorCells.xlsx");
    Workbook workbook = WorkbookFactory.create(file);
    WorkbookParserSettings settings = WorkbookParserSettings.builder()
        .withHeader(ExcelHeader.WITH_HEADER)
        .build();

    WorkbookParser parser = new WorkbookParser(settings, getContext(), workbook, "Sheet1::1");

    exception.expect(DataParserException.class);
    exception.expectMessage("EXCEL_PARSER_05 - Unsupported cell type ERROR");
    Record firstRow = parser.parse();
  }

  @Test
  public void testParseCorrectlyHandlesFilesWithHeaders() throws IOException, InvalidFormatException, DataParserException {
    InputStream file = getFile("/TestFile.xlsx");
    Workbook workbook = WorkbookFactory.create(file);
    WorkbookParserSettings settings = WorkbookParserSettings.builder()
        .withHeader(ExcelHeader.WITH_HEADER)
        .build();

    WorkbookParser parser = new WorkbookParser(settings, getContext(), workbook, "Sheet1::1");

    Record firstContentRow = parser.parse();

    LinkedHashMap<String, Field> contentMap = new LinkedHashMap<>();
    for (int i = 1; i <= 5; i++) {
      contentMap.put("column" + i, Field.create((double) i));
    }
    Field expected = Field.createListMap(contentMap);

    assertEquals(expected, firstContentRow.get());
  }

  @Test
  public void testParseCorrectlyHandlesFileThatIgnoresHeaders() throws IOException, DataParserException, InvalidFormatException {
    InputStream file = getFile("/TestFile.xlsx");
    Workbook workbook = WorkbookFactory.create(file);
    WorkbookParserSettings settings = WorkbookParserSettings.builder()
        .withHeader(ExcelHeader.IGNORE_HEADER)
        .build();

    WorkbookParser parser = new WorkbookParser(settings, getContext(), workbook, "Sheet1::1");

    Record firstContentRow = parser.parse();

    LinkedHashMap<String, Field> contentMap = new LinkedHashMap<>();
    for (int i = 0; i <= 4; i++) {
      contentMap.put(String.valueOf(i), Field.create((double) i + 1));
    }
    Field expected = Field.createListMap(contentMap);

    assertEquals(expected, firstContentRow.get());
  }

  @Test
  public void testParseCorrectlyHandlesFileWithNoHeaders() throws IOException, InvalidFormatException, DataParserException {
    InputStream file = getFile("/TestFile.xlsx");
    Workbook workbook = WorkbookFactory.create(file);
    WorkbookParserSettings settings = WorkbookParserSettings.builder()
        .withHeader(ExcelHeader.NO_HEADER)
        .build();

    WorkbookParser parser = new WorkbookParser(settings, getContext(), workbook, "Sheet1::0");

    Record firstContentRow = parser.parse();

    LinkedHashMap<String, Field> contentMap = new LinkedHashMap<>();
    for (int i = 0; i <= 4; i++) {
      contentMap.put(String.valueOf(i), Field.create("column" + (i + 1)));
    }
    Field expected = Field.createListMap(contentMap);

    assertEquals(expected, firstContentRow.get());
  }

  @Test
  public void testParseHandlesStartingFromANonZeroOffset() throws IOException, InvalidFormatException, DataParserException {
    InputStream file = getFile("/TestFileOffset.xlsx");
    Workbook workbook = WorkbookFactory.create(file);
    WorkbookParserSettings settings = WorkbookParserSettings.builder()
        .withHeader(ExcelHeader.IGNORE_HEADER)
        .build();

    WorkbookParser parser = new WorkbookParser(settings, getContext(), workbook, "Sheet2::2");

    Record firstContentRow = parser.parse();

    LinkedHashMap<String, Field> contentMap = new LinkedHashMap<>();
    for (int i = 0; i <= 2; i++) {
      contentMap.put(String.valueOf(i), Field.create((double) i + 4));
    }
    Field expected = Field.createListMap(contentMap);

    assertEquals(expected, firstContentRow.get());
  }

  @Test
  public void testParseHandlesMultipleSheets() throws IOException, InvalidFormatException, DataParserException {
    InputStream file = getFile("/TestFileMultipleSheets.xlsx");
    Workbook workbook = WorkbookFactory.create(file);
    WorkbookParserSettings settings = WorkbookParserSettings.builder()
        .withHeader(ExcelHeader.WITH_HEADER)
        .build();

    WorkbookParser parser = new WorkbookParser(settings, getContext(), workbook, "Sheet1::1");

    Record firstSheetFirstRow = parser.parse();
    Record firstSheetSecondRow = parser.parse();
    Record secondSheetFirstRow = parser.parse();
    Record secondSheetSecondRow = parser.parse();

    LinkedHashMap<String, Field> firstContentMap = new LinkedHashMap<>();
    for (int i = 1; i <= 5; i++) {
      firstContentMap.put("column" + i, Field.create((double) i));
    }
    Field expectedFirstRow = Field.createListMap(firstContentMap);

    LinkedHashMap<String, Field> secondContentMap = new LinkedHashMap<>();
    for (int i = 1; i <= 5; i++) {
      secondContentMap.put("column" + i, Field.create((double) i));
    }
    Field expectedSecondRow = Field.createListMap(secondContentMap);

    LinkedHashMap<String, Field> thirdContentMap = new LinkedHashMap<>();
    for (int i = 1; i <= 5; i++) {
      thirdContentMap.put("header" + i, Field.create((double) i*10));
    }
    Field expectedThirdRow = Field.createListMap(thirdContentMap);

    LinkedHashMap<String, Field> fourthContentMap = new LinkedHashMap<>();
    for (int i = 1; i <= 5; i++) {
      fourthContentMap.put("header" + i, Field.create((double) i*10));
    }
    Field expectedFourthRow = Field.createListMap(fourthContentMap);

    assertEquals(expectedFirstRow, firstSheetFirstRow.get());
    assertEquals(expectedSecondRow, firstSheetSecondRow.get());
    assertEquals(expectedThirdRow, secondSheetFirstRow.get());
    assertEquals(expectedFourthRow, secondSheetSecondRow.get());
  }

  @Test
  public void testParseHandlesBlanksCells() throws IOException, InvalidFormatException, DataParserException {
    InputStream file = getFile("/TestFileBlankCells.xlsx");
    Workbook workbook = WorkbookFactory.create(file);
    WorkbookParserSettings settings = WorkbookParserSettings.builder()
        .withHeader(ExcelHeader.WITH_HEADER)
        .build();

    WorkbookParser parser = new WorkbookParser(settings, getContext(), workbook, "Sheet1::1");

    Record recordFirstRow = parser.parse();

    LinkedHashMap<String, Field> firstContentMap = new LinkedHashMap<>();
    firstContentMap.put("column1", Field.create((double) 11));
    firstContentMap.put("column2", Field.create(""));
    firstContentMap.put("column3", Field.create(""));
    firstContentMap.put("column4", Field.create((double) 44));

    Field expectedFirstRow = Field.createListMap(firstContentMap);

    assertEquals(expectedFirstRow, recordFirstRow.get());
  }
}