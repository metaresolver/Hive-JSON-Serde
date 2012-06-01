/*======================================================================*
 * Copyright (c) 2011, OpenX Technologies, Inc. All rights reserved.    *
 *                                                                      *
 * Licensed under the New BSD License (the "License"); you may not use  *
 * this file except in compliance with the License. Unless required     *
 * by applicable law or agreed to in writing, software distributed      *
 * under the License is distributed on an "AS IS" BASIS, WITHOUT        *
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.     *
 * See the License for the specific language governing permissions and  *
 * limitations under the License. See accompanying LICENSE file.        *
 *======================================================================*/


package org.openx.data.jsonserde;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import org.openx.data.jsonserde.objectinspector.JsonObjectInspectorFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

/**
 * Properties:
 * ignore.malformed.json = true/false : malformed json will be ignored
 *         instead of throwing an exception
 *
 * @author rcongiu
 */
public class JsonSerDe implements SerDe {

    public static final Log LOG = LogFactory.getLog(JsonSerDe.class);

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectNode dummyNode = mapper.createObjectNode();

    List<String> columnNames;
    List<TypeInfo> columnTypes;
    StructTypeInfo rowTypeInfo;
    StructObjectInspector rowObjectInspector;
    boolean[] columnSortOrderIsDesc;
    List<String[]> columnPaths;

    // if set, will ignore malformed JSON in deserialization
    public static final String PROP_IGNORE_MALFORMED_JSON = "ignore.malformed.json";
    boolean ignoreMalformedJson = true;

    /**
     * Initializes the SerDe.
     * Gets the list of columns and their types from the table properties.
     * Will use them to look into/create JSON data.
     *
     * @param conf Hadoop configuration object
     * @param tbl  Table Properties
     * @throws SerDeException
     */
    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {
        LOG.debug("Initializing SerDe");

        // Get column names and sort order
        String columnNameProperty = tbl.getProperty(Constants.LIST_COLUMNS);
        String columnTypeProperty = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
        LOG.debug("columns " + columnNameProperty + " types " + columnTypeProperty);

        // table column names and types
        columnNames = Arrays.asList(columnNameProperty.split(","));
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

        if (columnNames.size() != columnTypes.size()) {
            throw new IllegalArgumentException("column name and type lists must be the same size");
        }

        // Create row related objects
        rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
        rowObjectInspector = (StructObjectInspector) JsonObjectInspectorFactory.getJsonObjectInspectorFromTypeInfo(rowTypeInfo);

        // Get the sort order
        String columnSortOrder = tbl.getProperty(Constants.SERIALIZATION_SORT_ORDER);
        columnSortOrderIsDesc = new boolean[columnNames.size()];
        for (int i = 0; i < columnSortOrderIsDesc.length; i++) {
            columnSortOrderIsDesc[i] = (columnSortOrder != null && columnSortOrder.charAt(i) == '-');
        }

        // other configuration
        ignoreMalformedJson = Boolean.parseBoolean(tbl.getProperty(PROP_IGNORE_MALFORMED_JSON, "true"));

	// override paths
	String paths = tbl.getProperty("paths");
	if (paths != null) {
	    columnPaths = new ArrayList<String[]>();
	    for (String p : paths.split(",")) {
		columnPaths.add(p.trim().split("\\."));
	    }
	    // TODO: assert columnPaths.size == columnNames.size
	}
    }

    @Override
    public Object deserialize(Writable w) throws SerDeException {
        if (!(w instanceof Text)) {
            throw new SerDeException("Writable is not instance of Text: " + w.getClass());
        }

        try {
	    if (columnPaths == null) {
		return mapper.readTree(w.toString());
	    } else {
		JsonNode root = mapper.readTree(w.toString());
		ObjectNode output = mapper.createObjectNode();
		for (int i = 0; i < columnPaths.size(); i++) {
		    String[] paths = columnPaths.get(i);
		    String columnName = columnNames.get(i);

		    JsonNode node = root;
		    for (String p : paths) {
			node = node.path(p);
		    }
		    if (!node.isMissingNode()) {
			output.put(columnName, node);
		    }
		}
		return output;
	    }
        }
        catch (IOException e) {
            if (ignoreMalformedJson) {
                LOG.warn("Ignoring malformed JSON: " + e.getMessage());
                return mapper.createObjectNode();
            }
            throw new SerDeException("Error parsing JSON", e);
        }
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowObjectInspector;
    }

    /**
     * We serialize to Text
     * @return {@link Text}
     */
    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    /**
     * Hive will call this to serialize an object. Returns a writable object
     * of the same class returned by {@link #getSerializedClass}
     *
     * @param obj The object to serialize
     * @param objInspector The ObjectInspector that knows about the object's structure
     * @return a serialized object in form of a Writable. Must be the
     *         same type returned by {@link #getSerializedClass}
     * @throws SerDeException
     */
    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
        // make sure it is a struct record
        if (objInspector.getCategory() != Category.STRUCT) {
            throw new SerDeException(getClass().toString()
                    + " can only serialize struct types, but we got: "
                    + objInspector.getTypeName());
        }

        JsonNode node = serializeStruct(obj, (StructObjectInspector) objInspector, columnNames);
        return new Text(node.toString());
    }

    private JsonNode serializeStruct(Object obj, StructObjectInspector soi, List<String> columnNames) {
        if (obj == null) {
            return null;
        }

        ObjectNode node = mapper.createObjectNode();
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        for (int i = 0; i < fields.size(); i++) {
            StructField sf = fields.get(i);
            Object data = soi.getStructFieldData(obj, sf);

            if (data != null) {
                // we want to serialize columns with their proper HIVE name,
                // not the _col2 kind of name usually generated upstream
                String name = (columnNames == null) ? sf.getFieldName() : columnNames.get(i);
                JsonNode value = serializeField(data, sf.getFieldObjectInspector());
                node.put(name, value);
            }
        }
        return node;
    }

    /**
     * Serializes a field. Since we have nested structures, it may be called
     * recursively for instance when defining a list<struct<>>
     *
     * @param obj Object holding the fields' content
     * @param oi  The field's objec inspector
     * @return content node
     */
    private JsonNode serializeField(Object obj, ObjectInspector oi) {
        if (obj == null) {
            return null;
        }

        switch (oi.getCategory()) {
            case PRIMITIVE:
                PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
                switch (poi.getPrimitiveCategory()) {
                    case VOID:
                        return dummyNode.nullNode();
                    case BOOLEAN:
                        return dummyNode.booleanNode(((BooleanObjectInspector) poi).get(obj));
                    case BYTE:
                        return dummyNode.numberNode(((ShortObjectInspector) poi).get(obj));
                    case DOUBLE:
                        return dummyNode.numberNode(((DoubleObjectInspector) poi).get(obj));
                    case FLOAT:
                        return dummyNode.numberNode(((FloatObjectInspector) poi).get(obj));
                    case INT:
                        return dummyNode.numberNode(((IntObjectInspector) poi).get(obj));
                    case LONG:
                        return dummyNode.numberNode(((LongObjectInspector) poi).get(obj));
                    case SHORT:
                        return dummyNode.numberNode(((ShortObjectInspector) poi).get(obj));
                    case STRING:
                        return dummyNode.textNode(((StringObjectInspector) poi).getPrimitiveJavaObject(obj));
                    default:
                        throw new IllegalStateException("Unhandled primitive: " + poi.getPrimitiveCategory());
                }
            case MAP:
                return serializeMap(obj, (MapObjectInspector) oi);
            case LIST:
                return serializeList(obj, (ListObjectInspector) oi);
            case STRUCT:
                return serializeStruct(obj, (StructObjectInspector) oi, null);
            default:
                throw new IllegalStateException("Unhandled category: " + oi.getCategory());
        }
    }

    private JsonNode serializeList(Object obj, ListObjectInspector loi) {
        if (obj == null) {
            return null;
        }

        ArrayNode node = mapper.createArrayNode();
        for (Object o : loi.getList(obj)) {
            node.add(serializeField(o, loi.getListElementObjectInspector()));
        }
        return node;
    }

    private JsonNode serializeMap(Object obj, MapObjectInspector moi) {
        if (obj == null) {
            return null;
        }

        ObjectNode node = mapper.createObjectNode();
        for (Map.Entry<?, ?> entry : moi.getMap(obj).entrySet()) {
            JsonNode name = serializeField(entry.getKey(), moi.getMapKeyObjectInspector());
            JsonNode value = serializeField(entry.getValue(), moi.getMapValueObjectInspector());
            node.put(name.toString(), value);
        }
        return node;
    }
}
