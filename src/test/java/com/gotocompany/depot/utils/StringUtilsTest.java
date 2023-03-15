package com.gotocompany.depot.utils;

import org.junit.Assert;
import org.junit.Test;

public class StringUtilsTest {

    @Test
    public void shouldReturnValidArgumentsForStringFormat() {
        Assert.assertEquals(0, StringUtils.countVariables("test"));
        Assert.assertEquals(0, StringUtils.countVariables(""));
        Assert.assertEquals(1, StringUtils.countVariables("test%dtest"));
        Assert.assertEquals(2, StringUtils.countVariables("test%dtest%ttest"));
        Assert.assertEquals(5, StringUtils.countVariables("test%dtest%ttest dskladja %s ds %d sdajk %b"));
    }

    @Test
    public void shouldReturnCharacterCount() {
        Assert.assertEquals(0, StringUtils.count("test", 'i'));
        Assert.assertEquals(0, StringUtils.count("", '5'));
        Assert.assertEquals(2, StringUtils.count("test", 't'));
        Assert.assertEquals(1, StringUtils.count("test", 'e'));
        Assert.assertEquals(1, StringUtils.count("test", 's'));
        Assert.assertEquals(0, StringUtils.count("test", '%'));
    }

    @Test
    public void shouldReturnValidArgsAndCharacters() {
        String testString = "test%s%ddjaklsjd%%%%s%y%d";
        Assert.assertEquals(8, StringUtils.count(testString, '%'));
        Assert.assertEquals(4, StringUtils.countVariables(testString));
    }
}
