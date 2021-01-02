package simpledb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UtilityTest {
    @Test
    public void ffs() {
        assertEquals(Utility.ffs((byte) 0b00000000), -1);
        assertEquals(Utility.ffs((byte) 0b00000001), 0);
        assertEquals(Utility.ffs((byte) 0b00000011), 0);
        assertEquals(Utility.ffs((byte) 0b00100010), 1);
        assertEquals(Utility.ffs((byte) 0b11111100), 2);
        assertEquals(Utility.ffs((byte) 0b11111111), 0);
        assertEquals(Utility.ffs((byte) 0b10000000), 7);
        assertEquals(Utility.ffs((byte) 0b11000000), 6);
        assertEquals(Utility.ffs((byte) 0b01000000), 6);
    }
}
