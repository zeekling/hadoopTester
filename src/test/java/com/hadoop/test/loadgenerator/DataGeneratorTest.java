package com.hadoop.test.loadgenerator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Unit tests for DataGenerator with empty input structure to ensure it handles gracefully */
public class DataGeneratorTest {

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void testDataGeneratorWithEmptyInputDoesNotFail() throws Exception {
    File inputDir = tmp.newFolder("inputEmpty");
    // Create empty dirStructure and fileStructure files
    new File(inputDir, StructureGenerator.DIR_STRUCTURE_FILE_NAME).createNewFile();
    new File(inputDir, StructureGenerator.FILE_STRUCTURE_FILE_NAME).createNewFile();

    File rootDir = tmp.newFolder("outputRoot");

    DataGenerator dg = new DataGenerator();
    String[] args = new String[] {
        "-inDir", inputDir.getAbsolutePath(),
        "-root", rootDir.getAbsolutePath()
    };

    int rc = dg.run(args);
    // Expect graceful exit with code 0
    assertEquals(0, rc);
  }
}
