package io.stargate.graphql.core;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class CaseUtil {
    //Conversion from go's strcase camel.go

    private static final Map uppercaseAcronyms = ImmutableMap.of("ID", "ID");

    /**
     * // ToCamel converts a string to CamelCase
     * func ToCamel(s string) string {
     * 	if uppercaseAcronym[s] {
     * 		s = strings.ToLower(s)
     *        }
     * 	return toCamelInitCase(s, true)
     * }
     */
    public static String toCamel(String s) {
        if (uppercaseAcronyms.containsKey(s)) {
            s = s.toLowerCase();
        }
        return toCamelInitCase(s, true);
    }

    /**
     * // ToLowerCamel converts a string to lowerCamelCase
     * func ToLowerCamel(s string) string {
     * 	if s == "" {
     * 		return s
     *        }
     * 	if uppercaseAcronym[s] {
     * 		s = strings.ToLower(s)
     *    }
     * 	if r := rune(s[0]); r >= 'A' && r <= 'Z' {
     * 		s = strings.ToLower(string(r)) + s[1:]
     *    }
     * 	return toCamelInitCase(s, false)
     * }
     */
    public static String toLowerCamel(String s) {
        if (s == null || s.equals("")) return s;
        if (uppercaseAcronyms.containsKey(s)) return s.toLowerCase();
        Character r = s.charAt(0);
        if (r >= 'A' && r <= 'Z') s = String.valueOf(r).toLowerCase() + s.substring(1);
        return toCamelInitCase(s, false);
    }

    /**
     * // Converts a string to CamelCase
     * func toCamelInitCase(s string, initCase bool) string {
     * 	s = addWordBoundariesToNumbers(s)
     * 	s = strings.Trim(s, " ")
     * 	n := ""
     * 	capNext := initCase
     * 	for _, v := range s {
     * 		if v >= 'A' && v <= 'Z' {
     * 			n += string(v)
     *                }
     * 		if v >= '0' && v <= '9' {
     * 			n += string(v)
     *        }
     * 		if v >= 'a' && v <= 'z' {
     * 			if capNext {
     * 				n += strings.ToUpper(string(v))
     *            } else {
     * 				n += string(v)
     *            }
     *        }
     * 		if v == '_' || v == ' ' || v == '-' || v == '.' {
     * 			capNext = true
     *        } else {
     * 			capNext = false
     *        }* 	}
     * 	return n
     * }
     */
    protected static String toCamelInitCase(String s, boolean initCase) {
        //s = addWordBoundariesToNumbers(s)
        s = s.trim();
        StringBuilder n = new StringBuilder();
        boolean capNext = initCase;
        for (char v : s.toCharArray()) {
            if (v >= 'A' && v <= 'Z') {
                n.append(String.valueOf(v));
            }
            if (v >= '0' && v <= '9') {
                n.append(String.valueOf(v));
            }
            if (v >= 'a' && v <= 'z') {
                if (capNext) {
                    n.append(String.valueOf(v).toUpperCase());
                } else {
                    n.append(String.valueOf(v));
                }
            }
            if (v == '_' || v == ' ' || v == '-' || v == '.') {
                capNext = true;
            } else {
                capNext = false;
            }
        }
        return n.toString();
    }
}
