package org.apache.parquet.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.BadConfigurationException;

public class ConfigurationUtil {

  public static Class<?> getClassFromConfig(Configuration configuration, String configName, Class<?> assignableFrom) {
    final String className = configuration.get(configName);
    if (className == null) {
      return null;
    }

    try {
      final Class<?> foundClass = configuration.getClassByName(className);
      if (!assignableFrom.isAssignableFrom(foundClass)) {
        throw new BadConfigurationException("class " + className + " set in job conf at "
                + configName + " is not a subclass of " + assignableFrom.getCanonicalName());
      }
      return foundClass;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("could not instantiate class " + className + " set in job conf at " + configName, e);
    }
  }

}
