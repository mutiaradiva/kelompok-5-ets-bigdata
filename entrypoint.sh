#!/bin/bash

# Convert environment variables to Hadoop configuration
# CORE_CONF_* -> core-site.xml
# HDFS_CONF_* -> hdfs-site.xml
# YARN_CONF_* -> yarn-site.xml

set -e

: ${HADOOP_CONF_DIR:=/opt/hadoop/etc/hadoop}

# Function to update config files
update_config() {
    local config_file=$1
    local prefix=$2
    
    # Create directory if doesn't exist
    mkdir -p $(dirname "$config_file")
    
    # Initialize XML if needed
    if [ ! -f "$config_file" ]; then
        cat > "$config_file" <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
</configuration>
EOF
    fi
}

# Update core-site.xml
update_config "$HADOOP_CONF_DIR/core-site.xml" "CORE_CONF"
for var in $(env | grep '^CORE_CONF_' | cut -d= -f1); do
    # Remove CORE_CONF_ prefix and convert underscores to dots
    prop_name=$(echo "${var#CORE_CONF_}" | sed 's/__/./g' | sed 's/_/./g')
    prop_value=$(eval echo \$$var)
    
    # Remove old property and add new one
    sed -i "/<name>$prop_name<\/name>/,/<\/property>/d" "$HADOOP_CONF_DIR/core-site.xml"
    
    # Insert property before closing tag
    sed -i "/<\/configuration>/i\\  <property>\\n    <name>$prop_name</name>\\n    <value>$prop_value</value>\\n  </property>" "$HADOOP_CONF_DIR/core-site.xml"
done

# Update hdfs-site.xml
update_config "$HADOOP_CONF_DIR/hdfs-site.xml" "HDFS_CONF"
for var in $(env | grep '^HDFS_CONF_' | cut -d= -f1); do
    prop_name=$(echo "${var#HDFS_CONF_}" | sed 's/__/./g' | sed 's/_/./g')
    prop_value=$(eval echo \$$var)
    
    sed -i "/<name>$prop_name<\/name>/,/<\/property>/d" "$HADOOP_CONF_DIR/hdfs-site.xml"
    sed -i "/<\/configuration>/i\\  <property>\\n    <name>$prop_name</name>\\n    <value>$prop_value</value>\\n  </property>" "$HADOOP_CONF_DIR/hdfs-site.xml"
done

# Update yarn-site.xml
update_config "$HADOOP_CONF_DIR/yarn-site.xml" "YARN_CONF"
for var in $(env | grep '^YARN_CONF_' | cut -d= -f1); do
    prop_name=$(echo "${var#YARN_CONF_}" | sed 's/__/./g' | sed 's/_/./g')
    prop_value=$(eval echo \$$var)
    
    sed -i "/<name>$prop_name<\/name>/,/<\/property>/d" "$HADOOP_CONF_DIR/yarn-site.xml"
    sed -i "/<\/configuration>/i\\  <property>\\n    <name>$prop_name</name>\\n    <value>$prop_value</value>\\n  </property>" "$HADOOP_CONF_DIR/yarn-site.xml"
done

# Execute the original command
exec "$@"
