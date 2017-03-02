﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json.Linq;
using System.Collections.ObjectModel;
using System.Diagnostics;

// Add DocumentDB references

namespace GraphView
{
    internal abstract class FieldObject
    {
        public static VertexPropertyField GetVertexPropertyField(JProperty property)
        {
            return new VertexPropertyField(property.Name, property.Value.ToString(),
                JsonDataTypeHelper.GetJsonDataType(property.Value.Type));
        }

        public static EdgePropertyField GetEdgePropertyField(JProperty property)
        {
            return new EdgePropertyField(property.Name, property.Value.ToString(),
                JsonDataTypeHelper.GetJsonDataType(property.Value.Type));
        }

        public static VertexField ConstructVertexField(GraphViewConnection connection, JObject vertexObject, Dictionary<string, JObject> edgeDocDict)
        {
            VertexField vertexField = new VertexField(connection);
            vertexField.JsonDocument = vertexObject;

            string vertexId = null;
            string vertexLabel = null;

            //
            // "_edge" & "_reverse_edge" could be either JObject or JArray:
            // - For vertexes that have numerous edges (too large to be filled in one document),
            //     they are JObject indicating the documents storing their in/out edges.
            //     The schema is defined in Schema.txt
            // - For small vertexes, they are JArray directly showing all the edges.
            //
            JToken forwardAdjList = null;
            JToken backwardAdjList = null;

            foreach (JProperty property in vertexObject.Properties()) {
                switch (property.Name) {
                // DocumentDB-reserved JSON properties
                case "_rid":
                case "_self":
                case "_etag":
                case "_attachments":
                case "_ts":
                    continue;
                case "id": // "id"
                    vertexId = property.Value.ToString();
                    vertexField.VertexProperties.Add(property.Name, GetVertexPropertyField(property));
                    break;
                case "label": // "label"
                    vertexLabel = property.Value.ToString();
                    vertexField.VertexProperties.Add(property.Name, GetVertexPropertyField(property));
                    break;
                case "_edge": // "_edge"
                    forwardAdjList = property.Value;
                    break;
                case "_reverse_edge": // "_reverse_edge"
                    backwardAdjList = property.Value;
                    break;
                default: // user-defined properties
                    vertexField.VertexProperties.Add(property.Name, GetVertexPropertyField(property));
                    break;
                }
            }

            Debug.Assert(forwardAdjList != null);
            if (forwardAdjList is JArray) {
                Debug.Assert(edgeDocDict == null, "Small vertexes should not have spilled edge-document");
                vertexField.AdjacencyList = GetForwardAdjacencyListField(vertexId, vertexLabel, (JArray)forwardAdjList);
            }
            else if (forwardAdjList is JObject) {
                Debug.Assert(edgeDocDict != null, "Large vertexes must have spilled edge-document");
                vertexField.AdjacencyList = GetForwardAdjacencyListField(vertexId, vertexLabel, connection, (JObject)forwardAdjList, edgeDocDict);
            }
            else {
                Debug.Assert(false, $"Should not get here! forwardAdjList is: {forwardAdjList.GetType()}");
            }


            Debug.Assert(backwardAdjList != null);
            if (backwardAdjList is JArray) {
                Debug.Assert(edgeDocDict == null, "Small vertexes should not have spilled edge-document");
                vertexField.RevAdjacencyList = GetBackwardAdjacencyListField(vertexId, vertexLabel, (JArray)backwardAdjList);
            }
            else if (backwardAdjList is JObject) {
                Debug.Assert(edgeDocDict != null, "Large vertexes must have spilled edge-document");
                vertexField.RevAdjacencyList = GetBackwardAdjacencyListField(vertexId, vertexLabel, connection, (JObject)backwardAdjList, edgeDocDict);
            }
            else {
                Debug.Assert(false, $"Should not get here! backwardAdjList is: {backwardAdjList.GetType()}");
            }

            return vertexField;
        }



        public static AdjacencyListField GetForwardAdjacencyListField(
            string outVId, string outVLabel, JArray edgeArray)
        {
            AdjacencyListField result = new AdjacencyListField();

            foreach (JObject edgeObject in edgeArray.Children<JObject>()) {
                result.AddEdgeField(outVId, (long)edgeObject["_offset"],
                                    EdgeField.ConstructForwardEdgeField(outVId, outVLabel, null, edgeObject));
            }

            return result;
        }

        public static AdjacencyListField GetBackwardAdjacencyListField(
            string inVId, string inVLabel, JArray edgeArray)
        {
            AdjacencyListField result = new AdjacencyListField();

            foreach (JObject edgeObject in edgeArray.Children<JObject>()) {
                result.AddEdgeField((string)edgeObject["_srcV"],  // for backward edge, this is the srcVertexId
                                    (long)edgeObject["_offset"],
                                    EdgeField.ConstructBackwardEdgeField(inVId, inVLabel, null, edgeObject));
            }

            return result;
        }


        /// <summary>
        /// For a vertex with lots of edges (thus can't be filled into one document), 
        /// "_edge" is JObject indicating the documents storing its (forward) edges.
        /// For the json schema of <paramref name="edgeContainer"/>, see Schema.txt
        /// </summary>
        /// <param name="outVId"></param>
        /// <param name="outVLabel"></param>
        /// <param name="connection"></param>
        /// <param name="edgeContainer"></param>
        /// <returns></returns>
        public static AdjacencyListField GetForwardAdjacencyListField(
            string outVId, string outVLabel, GraphViewConnection connection, JObject edgeContainer, Dictionary<string, JObject> edgeDocDict)
        {
            AdjacencyListField result = new AdjacencyListField();

            JArray edgeDocuments = (JArray)edgeContainer["_edges"];
            Debug.Assert(edgeDocuments != null, "edgeDocuments != null");

            foreach (JObject edgeDocument in edgeDocuments.Children<JObject>()) {
                string edgeDocID = (string)(JValue)edgeDocument["id"];
                Debug.Assert(!string.IsNullOrEmpty(edgeDocID), "!string.IsNullOrEmpty(edgeDocID)");

                //
                // Retreive edges from input dictionary: "id" == edgeDocID
                // Check: the metadata is right, and the "_edge" should not be null or empty 
                // (otherwise this edge-document should have been removed)
                //
                JObject edgeDocObject = edgeDocDict[edgeDocID];
                Debug.Assert(edgeDocObject != null, "edgeDocObject != null");
                Debug.Assert((bool)edgeDocObject["_is_reverse"] == false, "(bool)edgeDocObject['_is_reverse'] == false");
                Debug.Assert(((string)edgeDocObject["_vertex_id"]).Equals(outVId), "((string)edgeDocObject['_vertex_id']).Equals(outVId)");

                JArray edgesArray = (JArray)edgeDocObject["_edge"];
                Debug.Assert(edgesArray != null, "edgesArray != null");
                Debug.Assert(edgesArray.Count > 0, "edgesArray.Count > 0");
                foreach (JObject edgeObject in edgesArray.Children<JObject>()) {
                    result.AddEdgeField(outVId,
                                        (long)edgeObject["_offset"],
                                        EdgeField.ConstructForwardEdgeField(outVId, outVLabel, edgeDocID, edgeObject));
                }
            }

            return result;
        }


        /// <summary>
        /// For a vertex with lots of edges (thus can't be filled into one document), 
        /// "_reverse_edge" is JObject indicating the documents storing its (backward) edges.
        /// For the json schema of <paramref name="edgeContainer"/>, see Schema.txt
        /// </summary>
        /// <param name="inVId"></param>
        /// <param name="inVLabel"></param>
        /// <param name="connection"></param>
        /// <param name="edgeContainer"></param>
        /// <param name="edgeDocDict">Set of reverse-edge-documents for spilled vertexes</param>
        /// <returns></returns>
        public static AdjacencyListField GetBackwardAdjacencyListField(
            string inVId, string inVLabel, GraphViewConnection connection, JObject edgeContainer, Dictionary<string, JObject> edgeDocDict)
        {
            AdjacencyListField result = new AdjacencyListField();

            JArray edgeDocuments = (JArray)edgeContainer["_edges"];
            Debug.Assert(edgeDocuments != null, "edgeDocuments != null");

            foreach (JObject edgeDocument in edgeDocuments.Children<JObject>()) {
                string edgeDocID = (string)(JValue)edgeDocument["id"];
                Debug.Assert(!string.IsNullOrEmpty(edgeDocID), "!string.IsNullOrEmpty(edgeDocID)");

                //
                // Retreive edges from input dictionary: "id" == edgeDocID
                // Check: the metadata is right, and the "_edge" should not be null or empty 
                // (otherwise this edge-document should have been removed)
                //
                JObject edgeDocObject = edgeDocDict[edgeDocID];
                Debug.Assert(edgeDocObject != null, "edgeDocObject != null");
                Debug.Assert((bool)edgeDocObject["_is_reverse"] == true, "(bool)edgeDocObject['_is_reverse'] == true");
                Debug.Assert(((string)edgeDocObject["_vertex_id"]).Equals(inVId), "((string)edgeDocObject['_vertex_id']).Equals(outVId)");

                JArray edgesArray = (JArray)edgeDocObject["_edge"];
                Debug.Assert(edgesArray != null, "edgesArray != null");
                Debug.Assert(edgesArray.Count > 0, "edgesArray.Count > 0");
                foreach (JObject edgeObject in edgesArray.Children<JObject>()) {
                    result.AddEdgeField((string)edgeObject["_srcV"],
                                        (long)edgeObject["_offset"],
                                        EdgeField.ConstructBackwardEdgeField(inVId, inVLabel, edgeDocID, edgeObject));
                }
            }

            return result;
        }



        public virtual string ToGraphSON() => ToString();

        public virtual string ToValue => ToString();

    }

    internal class StringField : FieldObject
    {
        public string Value { get; set; }
        public JsonDataType JsonDataType { get; set; }

        public StringField(string value, JsonDataType jsonDataType = JsonDataType.String)
        {
            Value = value;
            JsonDataType = jsonDataType;
        }

        public override string ToString()
        {
            return Value;
        }

        public override string ToGraphSON()
        {
            if (JsonDataType == JsonDataType.String) 
               return "\"" + Value + "\"";
            return Value;
        }

        public override int GetHashCode()
        {
            return Value.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj)) return true;

            StringField stringField = obj as StringField;
            if (stringField == null)
            {
                return false;
            }

            return Value.Equals(stringField.Value);
        }

        public override string ToValue
        {
            get
            {
                return Value;
            }
        }
    }

    internal class CollectionField : FieldObject
    {
        public List<FieldObject> Collection { get; set; }

        public CollectionField()
        {
            Collection = new List<FieldObject>();
        }

        public CollectionField(List<FieldObject> collection)
        {
            Collection = collection;
        }

        public override string ToString()
        {
            if (Collection.Count == 0) return "[]";

            var collectionStringBuilder = new StringBuilder("[");
            collectionStringBuilder.Append(Collection[0].ToString());

            for (var i = 1; i < Collection.Count; i++)
                collectionStringBuilder.Append(", ").Append(Collection[i].ToString());

            collectionStringBuilder.Append(']');
            
            return collectionStringBuilder.ToString();
        }

        public override string ToGraphSON()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("[");

            for (int i = 0; i < Collection.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(", ");
                }
                sb.Append(Collection[i].ToGraphSON());
            }

            sb.Append("]");

            return sb.ToString();
        }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj)) return true;

            CollectionField colField = obj as CollectionField;
            if (colField == null || Collection.Count != colField.Collection.Count)
            {
                return false;
            }

            for (int i = 0; i < Collection.Count; i++)
            {
                if (!Collection[i].Equals(colField.Collection[i]))
                {
                    return false;
                }
            }

            return true;
        }

        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }
    }

    internal class MapField : FieldObject
    {
        public Dictionary<FieldObject, FieldObject> Map { get; set; }

        public MapField()
        {
            Map = new Dictionary<FieldObject, FieldObject>();
        }

        public MapField(Dictionary<FieldObject, FieldObject> map)
        {
            Map = map;
        }

        public override string ToString()
        {
            if (Map.Count == 0) return "[]";

            var mapStringBuilder = new StringBuilder("[");
            var i = 0;

            foreach (var pair in Map)
            {
                var key = pair.Key;
                var value = pair.Value;

                if (i++ > 0)
                    mapStringBuilder.Append(", ");
                mapStringBuilder.Append(key.ToString()).Append(":").Append(value.ToString());
            }

            mapStringBuilder.Append(']');

            return mapStringBuilder.ToString();
        }

        public override string ToGraphSON()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("{");

            bool firstEntry = true;
            foreach (var entry in Map)
            {
                if (firstEntry)
                {
                    firstEntry = false;
                }
                else
                {
                    sb.Append(", ");
                }

                sb.AppendFormat("{0}: {1}", entry.Key.ToGraphSON(), entry.Value.ToGraphSON());
            }

            sb.Append("}");
            return sb.ToString();
        }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj)) return true;

            MapField mapField = obj as MapField;
            if (mapField == null || Map.Count != mapField.Map.Count)
            {
                return false;
            }

            foreach (var kvp in Map)
            {
                var key = kvp.Key;
                FieldObject value2;
                if (!mapField.Map.TryGetValue(key, out value2))
                    return false;
                if (!kvp.Value.Equals(value2))
                    return false;
            }

            return true;
        }

        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }
    }

    internal class Compose1Field : FieldObject
    {
        public Dictionary<FieldObject, FieldObject> Map { get; set; }
        public FieldObject DefaultProjectionKey { get; set; }

        public Compose1Field(Dictionary<FieldObject, FieldObject> map, FieldObject defaultProjectionKey)
        {
            Map = map;
            DefaultProjectionKey = defaultProjectionKey;
        }

        public override string ToString()
        {
            return Map[DefaultProjectionKey].ToString();
        }

        public override string ToGraphSON()
        {
            return Map[DefaultProjectionKey].ToGraphSON();
        }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj)) return true;

            MapField mapField = obj as MapField;
            if (mapField == null || Map.Count != mapField.Map.Count)
            {
                return false;
            }

            foreach (var kvp in Map)
            {
                var key = kvp.Key;
                FieldObject value2;
                if (!mapField.Map.TryGetValue(key, out value2))
                    return false;
                if (!kvp.Value.Equals(value2))
                    return false;
            }

            return true;
        }

        public override int GetHashCode()
        {
            if (Map.Count == 0) return "[]".GetHashCode();

            var mapStringBuilder = new StringBuilder("[");
            var i = 0;

            foreach (var pair in Map)
            {
                var key = pair.Key;
                var value = pair.Value;

                if (i++ > 0)
                    mapStringBuilder.Append(", ");
                mapStringBuilder.Append(key.ToString()).Append(":[").Append(value.ToString()).Append(']');
            }

            mapStringBuilder.Append(']');

            return mapStringBuilder.ToString().GetHashCode();
        }
    }

    internal class PropertyField : FieldObject
    {
        public string PropertyName { get; private set; }
        public string PropertyValue { get; set; }
        public JsonDataType JsonDataType { get; set; }

        public PropertyField(string propertyName, string propertyValue, JsonDataType jsonDataType)
        {
            PropertyName = propertyName;
            PropertyValue = propertyValue;
            JsonDataType = jsonDataType;
        }

        public override string ToString()
        {
            return string.Format("{0}->{1}", PropertyName, PropertyValue);
        }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj)) return true;

            PropertyField pf = obj as PropertyField;
            if (pf == null)
            {
                return false;
            }

            return PropertyName == pf.PropertyName && PropertyValue == pf.PropertyValue;
        }

        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }

        public override string ToValue
        {
            get
            {
                return PropertyValue;
            }
        }

        public override string ToGraphSON()
        {
            if (JsonDataType == JsonDataType.String)
            {
                return string.Format("{{\"{0}\": \"{1}\"}}", PropertyName, PropertyValue);
            }
            return string.Format("{{\"{0}\": {1}}}", PropertyName, PropertyValue.ToLower());
        }
    }

    internal class VertexPropertyField : PropertyField
    {
        public VertexPropertyField(string propertyName, string propertyValue, JsonDataType jsonDataType) 
            : base(propertyName, propertyValue, jsonDataType)
        {
        }

        public override string ToString()
        {
            return string.Format("vp[{0}]", base.ToString());
        }
    }

    internal class EdgePropertyField : PropertyField
    {
        public EdgePropertyField(string propertyName, string propertyValue, JsonDataType jsonDataType) 
            : base(propertyName, propertyValue, jsonDataType)
        {
        }

        public override string ToString()
        {
            return string.Format("p[{0}]", base.ToString());
        }
    }


    internal class EdgeField : FieldObject
    {

        // <PropertyName, EdgePropertyField>
        public Dictionary<string, EdgePropertyField> EdgeProperties;

        public string Label { get; private set; }
        public string InVLabel { get; private set; }
        public string OutVLabel { get; private set; }
        public string InV { get; private set; }
        public string OutV { get; private set; }
        public string EdgeDocID { get; set; }
        public long Offset { get; private set; }

        private EdgeField()
        {
            this.EdgeProperties = new Dictionary<string, EdgePropertyField>();
        }

        public FieldObject this[string propertyName]
        {
            get
            {
                if (propertyName.Equals("*", StringComparison.OrdinalIgnoreCase))
                    return this;
                EdgePropertyField propertyField;
                this.EdgeProperties.TryGetValue(propertyName, out propertyField);
                return propertyField;
            }
        }


        //TODO: Refactor this code! not elegant!
        //TODO: Move all vertex/edge cache operations into VertexCache
        public void UpdateEdgeProperty(string propertyName, string propertyValue, JsonDataType jsonDataType)
        {
            EdgePropertyField propertyField;
            if (this.EdgeProperties.TryGetValue(propertyName, out propertyField))
            {
                propertyField.PropertyValue = propertyValue;
                propertyField.JsonDataType = jsonDataType;
            }
                
            else
                this.EdgeProperties.Add(propertyName, new EdgePropertyField(propertyName, propertyValue, jsonDataType));
        }

        public override string ToString()
        {
            return String.Format("e[{0}]{1}({2})-{3}->{4}({5})", this.EdgeProperties["_offset"].ToValue, this.OutV, this.OutVLabel, this.Label, this.InV, this.InVLabel);
        }

        public override string ToGraphSON()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("{{\"id\": {0}", this.EdgeProperties["_offset"].ToValue);
            if (this.Label != null) {
                sb.AppendFormat(", \"label\": \"{0}\"", this.Label);
            }

            sb.Append(", \"type\": \"edge\"");

            if (this.InVLabel != null) {
                sb.AppendFormat(", \"inVLabel\": \"{0}\"", this.InVLabel);
            }
            if (this.OutVLabel != null) {
                sb.AppendFormat(", \"outVLabel\": \"{0}\"", this.OutVLabel);
            }
            if (this.InV != null) {
                sb.AppendFormat(", \"inV\": \"{0}\"", this.InV);
            }
            if (this.OutV != null) {
                sb.AppendFormat(", \"outV\": \"{0}\"", this.OutV);
            }

            bool firstProperty = true;
            foreach (string propertyName in this.EdgeProperties.Keys) {
                switch (propertyName) {
                case "label":
                case "_offset":
                case "_srcV":
                case "_srcVLabel":
                case "_sinkV":
                case "_sinkVLabel":
                case "_edgeId":

                //case GremlinKeyword.EdgeSourceV:
                //case GremlinKeyword.EdgeSinkV:
                //case GremlinKeyword.EdgeOtherV:
                    continue;
                default:
                    break;
                }

                if (firstProperty) {
                    sb.Append(", \"properties\": {");
                    firstProperty = false;
                }
                else {
                    sb.Append(", ");
                }

                if (this.EdgeProperties[propertyName].JsonDataType == JsonDataType.String)
                {
                    sb.AppendFormat("\"{0}\": \"{1}\"", propertyName, this.EdgeProperties[propertyName].PropertyValue);
                }
                else
                {
                    sb.AppendFormat("\"{0}\": {1}", propertyName, this.EdgeProperties[propertyName].PropertyValue.ToLower());
                }
            }
            if (!firstProperty) {
                sb.Append("}");
            }
            sb.Append("}");

            return sb.ToString();
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(this, obj)) return true;

            EdgeField ef = obj as EdgeField;
            if (ef == null)
            {
                return false;
            }

            // TODO: Refactor
            return this.ToString().Equals(ef.ToString(), StringComparison.OrdinalIgnoreCase);
        }

        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }

        public static EdgeField ConstructForwardEdgeField(string outVId, string outVLabel, string edgeDocID, JObject edgeObject)
        {
            EdgeField edgeField = new EdgeField {
                OutV = outVId,
                OutVLabel = outVLabel,
                EdgeDocID = edgeDocID,
                Offset = (long)edgeObject["_offset"],
            };

            foreach (JProperty property in edgeObject.Properties()) {
                edgeField.EdgeProperties.Add(property.Name, GetEdgePropertyField(property));

                switch (property.Name) {
                case "_sinkV": // "_sinkV"
                    edgeField.InV = property.Value.ToString();
                    break;
                case "_sinkVLabel": // "_sinkVLabel"
                    edgeField.InVLabel = property.Value.ToString();
                    break;
                case "label":
                    edgeField.Label = property.Value.ToString();
                    break;
                }
            }

            return edgeField;
        }

        public static EdgeField ConstructBackwardEdgeField(string inVId, string inVLabel, string edgeDocID, JObject edgeObject)
        {
            EdgeField edgeField = new EdgeField {
                InV = inVId,
                InVLabel = inVLabel,
                EdgeDocID = edgeDocID,
                Offset = (long)edgeObject["_offset"],
            };

            foreach (JProperty property in edgeObject.Properties()) {
                edgeField.EdgeProperties.Add(property.Name, GetEdgePropertyField(property));

                switch (property.Name) {
                case "_srcV":
                    edgeField.OutV = property.Value.ToString();
                    break;
                case "_srcVLabel":
                    edgeField.OutVLabel = property.Value.ToString();
                    break;
                case "label":
                    edgeField.Label = property.Value.ToString();
                    break;
                }
            }

            return edgeField;
        }
    }

    internal class AdjacencyListField : FieldObject
    {
        // <$"{srcVertexId}.{edgeOffset}", EdgeField>
        private Dictionary<string, EdgeField> Edges { get; }

        public IEnumerable<EdgeField> AllEdges => this.Edges.Values;


        public AdjacencyListField()
        {
            this.Edges = new Dictionary<string, EdgeField>();
        }


        private string MakeKey(string srcVertexId, long edgeOffset) => $"{srcVertexId}.{edgeOffset}";


        public void AddEdgeField(string srcVertexId, long edgeOffset, EdgeField edgeField)
        {
            this.Edges.Add(MakeKey(srcVertexId, edgeOffset), edgeField);
        }

        public void RemoveEdgeField(string srcVertexId, long edgeOffset)
        {
            this.Edges.Remove(MakeKey(srcVertexId, edgeOffset));
        }

        public EdgeField GetEdgeField(string srcVertexId, long edgeOffset)
        {
            EdgeField edgeField;
            this.Edges.TryGetValue(MakeKey(srcVertexId, edgeOffset), out edgeField);
            return edgeField;
        }


        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();

            foreach (string offset in Edges.Keys.OrderBy(e => long.Parse(e)))
            {
                if (sb.Length > 0)
                {
                    sb.Append(", ");
                }
                sb.Append(Edges[offset].ToString());
            }

            return string.Format("[{0}]", sb.ToString());
        }

        public override string ToGraphSON()
        {
            StringBuilder sb = new StringBuilder();

            foreach (string offset in Edges.Keys.OrderBy(e => long.Parse(e)))
            {
                if (sb.Length > 0)
                {
                    sb.Append(", ");
                }
                sb.Append(Edges[offset].ToGraphSON());
            }

            return string.Format("[{0}]", sb.ToString());
        }
    }

    internal class VertexField : FieldObject
    {
        // <Property Name, VertexPropertyField>
        public Dictionary<string, VertexPropertyField> VertexProperties { get; set; }
        public AdjacencyListField AdjacencyList { get; set; }
        public AdjacencyListField RevAdjacencyList { get; set; }

        public JObject JsonDocument { get; set; }

        private GraphViewConnection connection;

        public FieldObject this[string propertyName]
        {
            get
            {
                if (propertyName.Equals("*", StringComparison.OrdinalIgnoreCase))
                    return this;
                else if (propertyName.Equals("_edge", StringComparison.OrdinalIgnoreCase))
                    return AdjacencyList;
                else if (propertyName.Equals("_reverse_edge", StringComparison.OrdinalIgnoreCase))
                    return RevAdjacencyList;
                else
                {
                    VertexPropertyField propertyField;
                    VertexProperties.TryGetValue(propertyName, out propertyField);
                    return propertyField;
                }
            }
        }

        public void UpdateVertexProperty(string propertyName, string propertyValue, JsonDataType jsonDataType)
        {
            VertexPropertyField propertyField;
            if (VertexProperties.TryGetValue(propertyName, out propertyField))
            {
                propertyField.PropertyValue = propertyValue;
                propertyField.JsonDataType = jsonDataType;
            }
            else
                VertexProperties.Add(propertyName, new VertexPropertyField(propertyName, propertyValue, jsonDataType));
        }

        public VertexField(GraphViewConnection connection)
        {
            VertexProperties = new Dictionary<string, VertexPropertyField>();
            AdjacencyList = new AdjacencyListField();
            RevAdjacencyList = new AdjacencyListField();

            this.connection = connection;
        }

        public override string ToString()
        {
            VertexPropertyField idProperty;
            string id;
            if (VertexProperties.TryGetValue("id", out idProperty))
            {
                id = idProperty.ToValue;
            }
            else
            {
                id = "";
            }
            return string.Format("v[{0}]", id);
        }

        public override string ToValue
        {
            get
            {
                VertexPropertyField idProperty;
                if (VertexProperties.TryGetValue("id", out idProperty))
                {
                    return idProperty.ToValue;
                }
                else
                {
                    return "";
                }
            }
        }

        public override string ToGraphSON()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("{");
            sb.AppendFormat("\"id\": \"{0}\"", VertexProperties["id"].PropertyValue);


            if (VertexProperties.ContainsKey("label"))
            {
                sb.Append(", ");
                sb.AppendFormat("\"label\": \"{0}\"", VertexProperties["label"].PropertyValue);
            }

            sb.Append(", ");
            sb.Append("\"type\": \"vertex\"");

            if (!connection.UseReverseEdges)
            {
                RevAdjacencyList = EdgeDocumentHelper.GetReverseAdjacencyListOfVertex(connection,
                    VertexProperties["id"].PropertyValue);
            }

            if (RevAdjacencyList != null && RevAdjacencyList.AllEdges.Any())
            {
                sb.Append(", \"inE\": {");
                // Groups incoming edges by their labels
                var groupByLabel = RevAdjacencyList.AllEdges.GroupBy(e => e["label"].ToValue);
                bool firstInEGroup = true;
                foreach (var g in groupByLabel)
                {
                    if (firstInEGroup)
                    {
                        firstInEGroup = false;
                    }
                    else
                    {
                        sb.Append(", ");
                    }

                    string edgelLabel = g.Key;
                    sb.AppendFormat("\"{0}\": [", edgelLabel);

                    bool firstInEdge = true;
                    foreach (EdgeField edgeField in g)
                    {
                        if (firstInEdge)
                        {
                            firstInEdge = false;
                        }
                        else
                        {
                            sb.Append(", ");
                        }

                        sb.Append("{");
                        sb.AppendFormat("\"id\": {0}, ", 
                            edgeField.EdgeProperties["_offset"].ToValue);
                        sb.AppendFormat("\"outV\": \"{0}\"", edgeField.OutV);

                        bool firstInEProperty = true;
                        foreach (string propertyName in edgeField.EdgeProperties.Keys)
                        {
                            switch(propertyName)
                            {
                            case "label":
                            case "_offset":
                            case "_srcV":
                            case "_sinkV":
                            case "_srcVLabel":
                            case "_sinkVLabel":
                                    continue;
                                default:
                                    break;
                            }

                            if (firstInEProperty)
                            {
                                sb.Append(", \"properties\": {");
                                firstInEProperty = false;
                            }
                            else
                            {
                                sb.Append(", ");
                            }

                            if (edgeField.EdgeProperties[propertyName].JsonDataType == JsonDataType.String)
                            {
                                sb.AppendFormat("\"{0}\": \"{1}\"",
                                propertyName,
                                edgeField.EdgeProperties[propertyName].PropertyValue);
                            } else
                            {
                                sb.AppendFormat("\"{0}\": {1}",
                                propertyName,
                                edgeField.EdgeProperties[propertyName].PropertyValue.ToLower());
                            }
                        }
                        if (!firstInEProperty)
                        {
                            sb.Append("}");
                        }
                        sb.Append("}");
                    }
                    sb.Append("]");
                }
                sb.Append("}");
            }

            if (AdjacencyList != null && AdjacencyList.AllEdges.Any())
            {
                sb.Append(", \"outE\": {");
                // Groups outgoing edges by their labels
                var groupByLabel = AdjacencyList.AllEdges.GroupBy(e => e["label"].ToValue);
                bool firstOutEGroup = true;
                foreach (var g in groupByLabel)
                {
                    if (firstOutEGroup)
                    {
                        firstOutEGroup = false;
                    }
                    else
                    {
                        sb.Append(", ");
                    }

                    string edgelLabel = g.Key;
                    sb.AppendFormat("\"{0}\": [", edgelLabel);

                    bool firstOutEdge = true;
                    foreach (EdgeField edgeField in g)
                    {
                        if (firstOutEdge)
                        {
                            firstOutEdge = false;
                        }
                        else
                        {
                            sb.Append(", ");
                        }

                        sb.Append("{");
                        sb.AppendFormat("\"id\": {0}, ", 
                            edgeField.EdgeProperties["_offset"].ToValue);
                        sb.AppendFormat("\"inV\": \"{0}\"", edgeField.InV);

                        bool firstOutEProperty = true;
                        foreach (string propertyName in edgeField.EdgeProperties.Keys)
                        {
                            switch (propertyName)
                            {
                            case "label":
                            case "_offset":
                            case "_srcV":
                            case "_sinkV":
                            case "_srcVLabel":
                            case "_sinkVLabel":
                                continue;
                                default:
                                    break;
                            }

                            if (firstOutEProperty)
                            {
                                sb.Append(", \"properties\": {");
                                firstOutEProperty = false;
                            }
                            else
                            {
                                sb.Append(", ");
                            }

                            if (edgeField.EdgeProperties[propertyName].JsonDataType == JsonDataType.String)
                            {
                                sb.AppendFormat("\"{0}\": \"{1}\"",
                                propertyName,
                                edgeField.EdgeProperties[propertyName].PropertyValue);
                            } else
                            {
                                sb.AppendFormat("\"{0}\": {1}",
                                propertyName,
                                edgeField.EdgeProperties[propertyName].PropertyValue.ToLower());
                            }
                        }
                        if (!firstOutEProperty)
                        {
                            sb.Append("}");
                        }
                        sb.Append("}");
                    }
                    sb.Append("]");
                }
                sb.Append("}");
            }

            bool firstVertexProperty = true;
            foreach (string propertyName in VertexProperties.Keys)
            {
                switch (propertyName) {
                case "id":
                case "label":
                case "_partition":
                case "_edge":
                case "_reverse_edge":
                case "_nextEdgeOffset":
                    continue;
                default:
                    break;
                }

                if (firstVertexProperty)
                {
                    sb.Append(", \"properties\": {");
                    firstVertexProperty = false;
                }
                else
                {
                    sb.Append(", ");
                }

                VertexPropertyField vp = VertexProperties[propertyName];

                if (vp.JsonDataType == JsonDataType.String)
                {
                    sb.AppendFormat("\"{0}\": [{{\"value\": \"{1}\"}}]", propertyName, vp.PropertyValue);

                }
                else
                {
                    sb.AppendFormat("\"{0}\": [{{\"value\": {1}}}]", propertyName, vp.PropertyValue.ToLower());
                }
            }
            if (!firstVertexProperty)
            {
                sb.Append("}");
            }

            sb.Append("}");

            return sb.ToString();
        }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj)) return true;

            VertexField vf = obj as VertexField;
            if (vf == null)
            {
                return false;
            }

            return this["id"].ToValue.Equals(vf["id"].ToValue, StringComparison.OrdinalIgnoreCase);
        }

        public override int GetHashCode()
        {
            return this["id"].ToValue.GetHashCode();
        }
    }

    internal class TreeField : FieldObject
    {
        private FieldObject nodeObject;
        internal Dictionary<FieldObject, TreeField> Children;

        internal TreeField(FieldObject pNodeObject)
        {
            nodeObject = pNodeObject;
            Children = new Dictionary<FieldObject, TreeField>();
        }

        public override string ToGraphSON()
        {
            // Don't print the dummy root
            StringBuilder strBuilder = new StringBuilder();
            int cnt = 0;
            strBuilder.Append("{");
            foreach (KeyValuePair<FieldObject, TreeField> child in Children)
            {
                if (cnt++ != 0)
                    strBuilder.Append(", ");

                child.Value.ToGraphSON(strBuilder);
            }
            strBuilder.Append("}");
            return strBuilder.ToString();
        }

        public void ToGraphSON(StringBuilder strBuilder)
        {
            int cnt = 0;
            strBuilder.Append("\"" + nodeObject.ToValue + "\":{\"key\":");
            strBuilder.Append(nodeObject.ToGraphSON());
            strBuilder.Append(", \"value\": ");
            strBuilder.Append("{");
            foreach (KeyValuePair<FieldObject, TreeField> child in Children)
            {
                if (cnt++ != 0)
                    strBuilder.Append(", ");

                child.Value.ToGraphSON(strBuilder);
            }
            strBuilder.Append("}}");
        }

        public override string ToString()
        {
            // Don't print the dummy root
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.Append("[");
            int cnt = 0;
            foreach (KeyValuePair<FieldObject, TreeField> child in Children)
            {
                if (cnt++ != 0)
                    strBuilder.Append(", ");
                child.Value.ToString(strBuilder);
            }
            strBuilder.Append("]");
            return strBuilder.ToString();
        }

        private void ToString(StringBuilder strBuilder)
        {
            strBuilder.Append(nodeObject.ToString()).Append(":[");
            var cnt = 0;
            foreach (var child in Children)
            {
                if (cnt++ != 0)
                    strBuilder.Append(", ");
                child.Value.ToString(strBuilder);
            }
            strBuilder.Append("]");
        }
    }


    /// <summary>
    /// RawRecord is a data sturcture representing data records flowing from one execution operator to another. 
    /// A data record is a multi-field blob. Each field is currently represented as a string.
    /// The interpretation of a record, i.e., the names of the fields/columns of the record, 
    /// is specified in the data operator producing them.  
    /// 
    /// The fields of a record produced by an execution operator are in two parts: 
    /// the first part contains k triples, each representing a node processed so far. 
    /// A triple describes: 1) the node ID, 2) the node's adjacency list, and 3) the node's reverse adjacency list.
    /// The second part is a list of node/edge properties of the processed nodes, projected by the SELECT clause. 
    /// 
    /// | node1 | node1_adjacency_list | node1_rev_adjacency_list |...| nodeK | nodeK_adjacency_list | nodeK_rev_adjacency_list | property1 | property2 |......
    /// </summary>
    internal class RawRecord
    {
        internal RawRecord()
        {
            fieldValues = new List<FieldObject>();
        }
        internal RawRecord(RawRecord rhs)
        {
            fieldValues = new List<FieldObject>(rhs.fieldValues);
        }
        internal RawRecord(int num)
        {
            fieldValues = new List<FieldObject>();
            for (int i = 0; i < num; i++)
            {
                fieldValues.Add(new StringField(""));
            }
        }

        public void Append(FieldObject fieldValue)
        {
            fieldValues.Add(fieldValue);
        }

        public void Append(RawRecord record)
        {
            fieldValues.AddRange(record.fieldValues);
        }

        public int Length
        {
            get
            {
                return fieldValues.Count;
            }
        }

        internal FieldObject RetriveData(List<string> header,string FieldName)
        {
            if (header.IndexOf(FieldName) == -1) return null;
            else if (fieldValues.Count <= header.IndexOf(FieldName)) return null;
            else return fieldValues[header.IndexOf(FieldName)];
        }
        internal FieldObject RetriveData(int index)
        {
            return fieldValues[index];
        }

        internal FieldObject this[int index]
        {
            get
            {
                return fieldValues[index];
            }
        }

        //internal int RetriveIndex(string value)
        //{
        //    if (fieldValues.IndexOf(value) == -1) return -1;
        //    else return fieldValues.IndexOf(value);
        //}
        //internal String RetriveRow()
        //{
        //    String row = "";
        //    if (fieldValues == null) return row;
        //    for(int i = 0; i < fieldValues.Count; i++)
        //    {
        //        row += fieldValues[i].ToString() + ",";
        //    }
        //    return row;
        //}
        internal List<FieldObject> fieldValues;
    }

    /// <summary>
    /// Record differs from RawRecord in that the field names of the blob is annotated. 
    /// It is hence comprehensible to external data readers.  
    /// </summary>
    //public class Record
    //{
    //    RawRecord rawRecord;

    //    internal Record(RawRecord rhs, List<string> pHeader)
    //    {
    //        if (rhs != null)
    //        {
    //            rawRecord = rhs;
    //            header = pHeader;
    //        }
    //    }
    //    internal List<string> header { get; set; }
    //    public string this[int index]
    //    {
    //        get
    //        {
    //            if (index >= rawRecord.fieldValues.Count)
    //                throw new IndexOutOfRangeException("Out of range," + "the Record has only " + rawRecord.fieldValues.Count + " fields");
    //            else return rawRecord.fieldValues[index].ToString();
    //        }
    //    }

    //    public string this[string FieldName]
    //    {
    //        get
    //        {
    //            if (header == null || header.IndexOf(FieldName) == -1) 
    //                throw new IndexOutOfRangeException("Out of range," + "the Record has no field \"" + FieldName + "\".");
    //            else return rawRecord.fieldValues[header.IndexOf(FieldName)].ToString();
    //        }
    //    }
    //}

    internal enum GraphViewEdgeTableReferenceEnum
    {
        BothE,
        OutE,
        InE
    }

    internal enum GraphViewVertexTableReferenceEnum
    {
        Both,
        OutV,
        InV
    }

    internal class GraphViewReservedProperties
    {
        internal static ReadOnlyCollection<string> ReservedNodeProperties { get; } = 
            new ReadOnlyCollection<string>(new List<string> { "id", "label", "_edge", "_reverse_edge", "*" });

        internal static ReadOnlyCollection<string> ReservedEdgeProperties { get; } =
            new ReadOnlyCollection<string>(new List<string> {"_source", "_sink", "_other", "_offset", "*"});
    }

    /// <summary>
    /// The interface of query execution operators.
    /// An operator is in one of the states: open or closed. 
    /// By implementing Next(), a query execution operator implements its own computation logic 
    /// and returns result iteratively. 
    /// </summary>
    internal interface IGraphViewExecution
    {
        bool State();
        void Open();
        void Close();
        RawRecord Next();
    }
    /// <summary>
    /// The base class for all query execution operators. 
    /// The class implements the execution interface and specifies the field names of 
    /// the raw records produced by this operator. 
    /// </summary>
    internal abstract class GraphViewExecutionOperator : IGraphViewExecution
    {
        private bool state;
        public bool State()
        {
            return state;
        }
        public void Open()
        {
            state = true;
        }
        public void Close()
        {
            state = false;
        }
        public virtual void ResetState()
        {
            this.Open();
        }
        public abstract RawRecord Next();

        protected Dictionary<WColumnReferenceExpression, int> privateRecordLayout;

        // Number of vertices processed so far
        internal int NumberOfProcessedVertices;


        //
        // [Obsolete]: Use GraphViewConnection.ExecuteQuery()
        //
        //internal static IQueryable<dynamic> SendQuery(string script, GraphViewConnection connection)
        //{
        //    FeedOptions QueryOptions = new FeedOptions { MaxItemCount = -1 };
        //    IQueryable<dynamic> Result = connection.DocDBClient.CreateDocumentQuery(
        //        UriFactory.CreateDocumentCollectionUri(connection.DocDBDatabaseId, connection.DocDBCollectionId), 
        //        script, QueryOptions);
        //    return Result;
        //}
    }
}
