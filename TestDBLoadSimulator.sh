#!/bin/sh
. ./TestDBSimulator.properties

echo "***************************************************"
echo "creating the table usertable with regionsplits..."
echo "***************************************************"

cd $TESTDB_SIMULATOR_HOME
javac -classpath .:./lib/* TestSchemaInitializer.java
java -classpath .:./lib/* TestSchemaInitializer TestDBSimulator.properties > TestDBLoadSimulator.log  2>&1

echo "Initialized Test DB Schema."

echo "***************************************************"
echo "Running YCSB to enter the OrganisationID's..."
echo "***************************************************"
cd $YCSB_HOME
./bin/ycsb $YCSB_OPERATION hbase -P $WORKLOAD -p columnfamily=$COLUMN_FAMILY -p table=$TABLE_NAME -p orgCount=$ORGANIZATIONS_COUNT -p includeOrgId=$INCLUDE_ORGID >> $TESTDB_SIMULATOR_HOME/TestDBLoadSimulator.log 2>&1

echo "Test DB Load Initiated."
echo "***************************************************"
