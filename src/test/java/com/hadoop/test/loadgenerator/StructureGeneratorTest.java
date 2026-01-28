package com.hadoop.test.loadgenerator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for StructureGenerator
 */
public class StructureGeneratorTest {

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void testStructureGeneratorOutputsFilesWithSeed() throws Exception {
    // Arrange: output to a temporary directory with a fixed seed
    File outDir = tmp.newFolder("structureOut");
    StructureGenerator sg = new StructureGenerator();
    String[] args = new String[] {
        "-maxDepth", "3",
        "-minWidth", "1",
        "-maxWidth", "3",
        "-numOfFiles", "4",
        "-avgFileSize", "1",
        "-outDir", outDir.getAbsolutePath(),
        "-seed", "12345"
    };

    // Act
    int rc = sg.run(args);

    // Assert
    assertEquals(0, rc);
    File dirStruct = new File(outDir, StructureGenerator.DIR_STRUCTURE_FILE_NAME);
    File fileStruct = new File(outDir, StructureGenerator.FILE_STRUCTURE_FILE_NAME);
    assertTrue("dirStructure file should exist", dirStruct.exists());
    assertTrue("fileStructure file should exist", fileStruct.exists());

    List<String> dirLines = Files.readAllLines(dirStruct.toPath(), StandardCharsets.UTF_8);
    List<String> fileLines = Files.readAllLines(fileStruct.toPath(), StandardCharsets.UTF_8);

    // There should be some leaf directories and exactly numOfFiles lines for file structure
    assertTrue("dirStructure should contain at least one leaf", dirLines.size() > 0);
    assertEquals("fileStructure should contain 4 lines", 4, fileLines.size());

    // Validate each file line has two tokens: path and size, and size is a positive number
    for (String line : fileLines) {
      String[] tokens = line.trim().split("\\s+");
      assertEquals("Each line should have 2 tokens", 2, tokens.length);
      String sizeToken = tokens[1];
      try {
        double v = Double.parseDouble(sizeToken);
        assertTrue("File size should be positive", v > 0);
      } catch (NumberFormatException e) {
        throw new AssertionError("Invalid file size: " + sizeToken, e);
      }
      assertTrue("Path should not be empty", tokens[0] != null && tokens[0].trim().length() > 0);
    }
  }
}
