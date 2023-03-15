package com.gotocompany.depot.config.converter;

import com.google.common.base.Strings;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.exception.InvalidTemplateException;
import org.aeonbits.owner.Converter;
import org.json.JSONObject;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TemplateMapConverter implements Converter<Map<Template, Template>> {

    @Override
    public Map<Template, Template> convert(Method method, String input) {
        if (Strings.isNullOrEmpty(input)) {
            return Collections.emptyMap();
        }
        JSONObject jsonObject = new JSONObject(input);
        Map<Template, Template> templateMap = new HashMap<>();
        Iterator<String> keys = jsonObject.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            try {
                templateMap.put(new Template(key), new Template(jsonObject.get(key).toString()));
            } catch (InvalidTemplateException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }
        return templateMap;
    }
}
