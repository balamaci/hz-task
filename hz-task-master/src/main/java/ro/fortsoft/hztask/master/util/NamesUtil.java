package ro.fortsoft.hztask.master.util;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author Serban Balamaci
 */
public class NamesUtil {

    private static final Map<String, String> UUID_TO_NAMES = Maps.newConcurrentMap();

    public static void addMember(String uuid, String name) {
        UUID_TO_NAMES.put(uuid, name);
    }

    public static String getName(String uuid) {
        return UUID_TO_NAMES.get(uuid);
    }

    public static String getNameWithUuidFallback(String uuid) {
        String name = getName(uuid);
        return name == null ? uuid : name;
    }

    public static String toLogFormat(String uuid) {
        String name = getName(uuid);
        if(name == null) {
            name = "????";
        }

        return name + "(" + uuid.substring(0, 8) + ")";
    }

}
