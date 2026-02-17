import os
from datetime import datetime

import pandas as pd
from pyairtable import Api, Table
from pyairtable.models.schema import  FieldSchema, TableSchema
from pydantic import Field,ConfigDict
from dagster import asset, get_dagster_logger, define_asset_job, ConfigurableResource
class ResourceWithAirtableConfiguration(ConfigurableResource):

    # this should be s3, since it is the s3 resource. Others at gleaner s3 resources
    AIRTABLE_ACCESS_TOKEN: str =  Field(
         description="AIRTABLE_ACCESS_TOKEN")
    AIRTABLE_BASE_ID: str =  Field(
         description="AIRTABLE_BASE_ID")


class AirtableResource( ResourceWithAirtableConfiguration):

    def getClient(self) -> Api:
        return Api(self.AIRTABLE_ACCESS_TOKEN)

    def getTable(self, tableid) -> Table:
        return self.getClient().table(self.AIRTABLE_BASE_ID, tableid)

    def getTableFields(self, tableid) -> list[FieldSchema]:
        return self.getClient().table(self.AIRTABLE_BASE_ID, tableid).schema().fields

    def upsert2Table(self, tableid: str, df: pd.DataFrame, keyfields=None, linked_field_mappings=None) -> list:
        """
        Upsert data to Airtable table with support for linked field mapping

        Args:
            tableid: Airtable table ID
            df: DataFrame to upsert
            keyfields: Fields to use as unique keys for upsert
            linked_field_mappings: List of dictionaries for linked field mapping, each containing:
                - field_in_record: Name of field in the record to be replaced
                - filtered_records: Array of records from linked table
                - name_to_match_in_table: Field name in linked table to match against
        """
        records = []
        get_dagster_logger().info(f"airtable dataframe types: {df.dtypes}")
        df.fillna('', inplace=True)

        for key, row in df.iterrows():
            fields = row.to_dict()

            # Process linked field mappings if provided
            if linked_field_mappings:
                for mapping in linked_field_mappings:
                    field_in_record = mapping['field_in_record']
                    filtered_records = mapping['filtered_records']
                    name_to_match_in_table = mapping['name_to_match_in_table']

                    # Check if this field exists in the current record
                    if field_in_record in fields and fields[field_in_record]:
                        value_to_match = fields[field_in_record]

                        # Find matching record ID in filtered_records
                        matching_record_id = None
                        for record in filtered_records:
                            if record.get('fields', {}).get(name_to_match_in_table) == value_to_match:
                                matching_record_id = record['id']
                                break

                    # Replace the field value with the record ID
                    if matching_record_id:
                        fields[field_in_record] = [matching_record_id]  # Airtable linked fields expect arrays
                        get_dagster_logger().debug(f"Replaced {field_in_record} value '{value_to_match}' with record ID '{matching_record_id}'")
                    else:
                        get_dagster_logger().warning(f"No matching record found for {field_in_record} value '{value_to_match}' in field '{name_to_match_in_table}'")
                        # Optionally remove the field or set to empty
                        fields[field_in_record] = []

        get_dagster_logger().debug(fields)
        arecord = { 'fields': fields }
        records.append(arecord)

        try:
            ids = self.getTable(tableid).batch_upsert(records, keyfields, typecast=True)
            return ids
        except Exception as e:
            get_dagster_logger().error(f"airtable failed upsert {e}")
            return []

    def emptyTable(self, tableid: str) -> bool:
        """
        Delete all records from an Airtable table

        Args:
            tableid: Airtable table ID

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            table = self.getTable(tableid)

            # Get all record IDs
            all_records = table.all(fields=[])  # Only get IDs, no field data needed

            if not all_records:
                get_dagster_logger().info(f"Table {tableid} is already empty")
                return True

            record_ids = [record['id'] for record in all_records]
            get_dagster_logger().info(f"Deleting {len(record_ids)} records from table {tableid}")

            # Airtable API allows batch deletion of up to 10 records at a time
            batch_size = 10
            deleted_count = 0

            for i in range(0, len(record_ids), batch_size):
                batch_ids = record_ids[i:i + batch_size]
                try:
                    table.batch_delete(batch_ids)
                    deleted_count += len(batch_ids)
                    get_dagster_logger().debug(f"Deleted batch of {len(batch_ids)} records")
                except Exception as e:
                    get_dagster_logger().error(f"Failed to delete batch {i//batch_size + 1}: {e}")
                    return False

            get_dagster_logger().info(f"Successfully deleted {deleted_count} records from table {tableid}")
            return True

        except Exception as e:
            get_dagster_logger().error(f"Failed to empty table {tableid}: {e}")
            return False

    def upsert2Table(self, tableid: str, df: pd.DataFrame, keyfields=None, linked_field_mappings=None, empty_first=False) -> list:
        """
        Upsert data to Airtable table with support for linked field mapping

        Args:
            tableid: Airtable table ID
            df: DataFrame to upsert
            keyfields: Fields to use as unique keys for upsert
            linked_field_mappings: List of dictionaries for linked field mapping
            empty_first: If True, delete all records from table before upserting
        """
        # Empty table first if requested
        if empty_first:
            if not self.emptyTable(tableid):
                get_dagster_logger().error("Failed to empty table, aborting upsert")
                return []

        records = []
        get_dagster_logger().info(f"airtable dataframe types: {df.dtypes}")

        # Clean up the DataFrame
        df_clean = df.copy()
        df_clean.fillna('', inplace=True)

        # Convert data types to Airtable-friendly formats
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':
                # Convert object columns to strings and handle NaN/None
                df_clean[col] = df_clean[col].astype(str).replace(['nan', 'None', 'NaT'], '')
            elif pd.api.types.is_datetime64_any_dtype(df_clean[col]):
                # Convert datetime to ISO format strings
                df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d').fillna('')
            elif pd.api.types.is_numeric_dtype(df_clean[col]):
                # Ensure numeric fields are properly handled
                df_clean[col] = df_clean[col].fillna(0)
                # Convert numpy types to native Python types
                if pd.api.types.is_integer_dtype(df_clean[col]):
                    df_clean[col] = df_clean[col].astype('Int64').fillna(0)
                else:
                    df_clean[col] = df_clean[col].astype(float).fillna(0.0)

        for key, row in df_clean.iterrows():
            fields = {}

            # Convert row to dict and handle data types
            for field_name, value in row.items():
                if pd.isna(value):
                    fields[field_name] = ''
                elif isinstance(value, (pd.Timestamp, datetime)):
                    fields[field_name] = value.strftime('%Y-%m-%d')
                elif isinstance(value, (int, float)) and pd.isna(value):
                    fields[field_name] = 0
                elif hasattr(value, 'item'):  # Handle numpy scalars
                    fields[field_name] = value.item()
                else:
                    fields[field_name] = str(value) if value != '' else ''

            # Process linked field mappings if provided
            if linked_field_mappings:
                for mapping in linked_field_mappings:
                    field_in_record = mapping['field_in_record']
                    filtered_records = mapping['filtered_records']
                    name_to_match_in_table = mapping['name_to_match_in_table']

                    # Check if this field exists in the current record
                    if field_in_record in fields and fields[field_in_record] and fields[field_in_record] != '':
                        value_to_match = str(fields[field_in_record]).strip()

                        # Find matching record ID in filtered_records
                        matching_record_id = None
                        for record in filtered_records:
                            record_value = record.get('fields', {}).get(name_to_match_in_table)
                            if record_value and str(record_value).strip() == value_to_match:
                                matching_record_id = record['id']
                                break

                        # Replace the field value with the record ID
                        if matching_record_id:
                            fields[field_in_record] = [matching_record_id]  # Airtable linked fields expect arrays
                            get_dagster_logger().debug(f"Replaced {field_in_record} value '{value_to_match}' with record ID '{matching_record_id}'")
                        else:
                            get_dagster_logger().warning(f"No matching record found for {field_in_record} value '{value_to_match}' in field '{name_to_match_in_table}'")
                            # Remove the field to avoid upsert issues
                            fields.pop(field_in_record, None)

            get_dagster_logger().debug(f"Final record fields: {fields}")
            arecord = { 'fields': fields }
            records.append(arecord)

        # Clean up keyfields to ensure they're strings
        if keyfields:
            clean_keyfields = [str(field) for field in keyfields]
            get_dagster_logger().info(f"Using keyfields: {clean_keyfields}")
        else:
            clean_keyfields = keyfields

        try:
            ids = self.getTable(tableid).batch_upsert(records, clean_keyfields, typecast=True)
            return ids
        except Exception as e:
            get_dagster_logger().error(f"airtable failed upsert {e}")
            # Log a sample record for debugging
            if records:
                get_dagster_logger().error(f"Sample record causing error: {records[0]}")
            return []

            # ... existing code ...
            return []

    def batch_create2Table(self, tableid: str, df: pd.DataFrame, linked_field_mappings=None) -> list:
        """
        Batch create records in Airtable table from DataFrame

        Args:
            tableid: Airtable table ID
            df: DataFrame to create records from
            linked_field_mappings: List of dictionaries for linked field mapping, each containing:
                - field_in_record: Name of field in the record to be replaced
                - filtered_records: Array of records from linked table
                - name_to_match_in_table: Field name in linked table to match against

        Returns:
            list: List of created record IDs
        """
        records = []
        get_dagster_logger().info(f"airtable batch_create dataframe types: {df.dtypes}")

        # Clean up the DataFrame
        df_clean = df.copy()
        df_clean.fillna('', inplace=True)

        # Convert data types to Airtable-friendly formats
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':
                # Convert object columns to strings and handle NaN/None
                df_clean[col] = df_clean[col].astype(str).replace(['nan', 'None', 'NaT'], '')
            elif pd.api.types.is_datetime64_any_dtype(df_clean[col]):
                # Convert datetime to ISO format strings
                df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d').fillna('')
            elif pd.api.types.is_numeric_dtype(df_clean[col]):
                # Ensure numeric fields are properly handled
                df_clean[col] = df_clean[col].fillna(0)
                # Convert numpy types to native Python types
                if pd.api.types.is_integer_dtype(df_clean[col]):
                    #df_clean[col] = df_clean[col].astype('Int64').fillna(0)
                    df_clean[col] = df_clean[col].astype(float).fillna(0)
                else:
                    df_clean[col] = df_clean[col].astype(float).fillna(0.0)

        for key, row in df_clean.iterrows():
            fields = {}

            # Convert row to dict and handle data types
            for field_name, value in row.items():
                if pd.isna(value):
                    fields[field_name] = ''
                elif isinstance(value, (pd.Timestamp, datetime)):
                    fields[field_name] = value.strftime('%Y-%m-%d')
                elif isinstance(value, (int, float)) and pd.isna(value):
                    fields[field_name] = 0
                elif hasattr(value, 'item'):  # Handle numpy scalars
                    fields[field_name] = value.item()
                else:
                    fields[field_name] = str(value) if value != '' else ''

            # Process linked field mappings if provided
            if linked_field_mappings:
                for mapping in linked_field_mappings:
                    field_in_record = mapping['field_in_record']
                    filtered_records = mapping['filtered_records']
                    name_to_match_in_table = mapping['name_to_match_in_table']

                    # Check if this field exists in the current record
                    if field_in_record in fields and fields[field_in_record] and fields[field_in_record] != '':
                        value_to_match = str(fields[field_in_record]).strip()

                        # Find matching record ID in filtered_records
                        matching_record_id = None
                        for record in filtered_records:
                            record_value = record.get('fields', {}).get(name_to_match_in_table)
                            if record_value and str(record_value).strip() == value_to_match:
                                matching_record_id = record['id']
                                break

                        # Replace the field value with the record ID
                        if matching_record_id:
                            fields[field_in_record] = [matching_record_id]  # Airtable linked fields expect arrays
                            get_dagster_logger().debug(
                                f"Replaced {field_in_record} value '{value_to_match}' with record ID '{matching_record_id}'")
                        else:
                            get_dagster_logger().warning(
                                f"No matching record found for {field_in_record} value '{value_to_match}' in field '{name_to_match_in_table}'")
                            # Remove the field to avoid creation issues
                            fields.pop(field_in_record, None)

            get_dagster_logger().debug(f"Final record fields: {fields}")
            arecord = fields
            records.append(arecord)

        try:
            created_records = self.getTable(tableid).batch_create(records, typecast=True)
            ids = [record['id'] for record in created_records]
            get_dagster_logger().info(f"Successfully created {len(ids)} records in table {tableid}")
            return ids
        except Exception as e:
            get_dagster_logger().error(f"airtable failed batch_create {e}")
            # Log a sample record for debugging
            if records:
                get_dagster_logger().error(f"Sample record causing error: {records[0]}")
            raise e
