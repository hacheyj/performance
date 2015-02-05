using System.IO;
using System.Runtime.Serialization;
using Microsoft.Hadoop.Avro;
using Microsoft.Hadoop.Avro.Container;
using Microsoft.Hadoop.Avro.Schema;
using Newtonsoft.Json;
using NUnit.Framework;
using Performance.Core;

namespace Serialization
{
    [TestFixture]
    public class SerializationPerformanceTests
    {
        // Portions of the following are based on http://azure.microsoft.com/en-us/documentation/articles/hdinsight-dotnet-avro-serialization/       
        [DataContract(Name = "SensorDataValue", Namespace = "Sensors")]
        internal class SensorData
        {
            [DataMember(Name = "Location")]
            public Location Position { get; set; }

            [DataMember(Name = "Value")]
            public byte[] Value { get; set; }
        }

        [DataContract]
        internal struct Location
        {
            [DataMember]
            public int Floor { get; set; }

            [DataMember]
            public int Room { get; set; }
        }

        private SensorData CreateTestData()
        {
            return new SensorData {Value = new byte[] {1, 2, 3, 4, 5}, Position = new Location {Room = 243, Floor = 1}};
        }

        private dynamic CreateDynamicData(RecordSchema recordSchema)
        {
            dynamic expected1 = new AvroRecord(recordSchema);
            dynamic location1 = new AvroRecord(recordSchema.GetField("Location").TypeSchema);
            location1.Floor = 1;
            location1.Room = 243;
            expected1.Location = location1;
            expected1.Value = new byte[] {1, 2, 3, 4, 5};

            return expected1;
        }

        [Test]
        public void TestAvroSerializationThroughReflection()
        {
            SensorData sensorData = CreateTestData();

            PerformanceHarness.Test(() =>
            {
                using (var buffer = new MemoryStream())
                {
                    using (var avroWriter = AvroContainer.CreateWriter<SensorData>(buffer, Codec.Deflate))
                    {
                        using (var writer = new SequentialWriter<SensorData>(avroWriter, 24))
                        {
                            writer.Write(sensorData);
                        }
                    }
                }

            }, "Avro serialization through reflection", 10000);
        }

        [Test]
        public void TestAvroSerializeUsingObjectContainersGenericRecord()
        {
            const string schema = @"{
                            ""type"":""record"",
                            ""name"":""Microsoft.Hadoop.Avro.Specifications.SensorData"",
                            ""fields"":
                                [
                                    { 
                                        ""name"":""Location"", 
                                        ""type"":
                                            {
                                                ""type"":""record"",
                                                ""name"":""Microsoft.Hadoop.Avro.Specifications.Location"",
                                                ""fields"":
                                                    [
                                                        { ""name"":""Floor"", ""type"":""int"" },
                                                        { ""name"":""Room"", ""type"":""int"" }
                                                    ]
                                            }
                                    },
                                    { ""name"":""Value"", ""type"":""bytes"" }
                                ]
                        }";

            //Create a generic serializer based on the schema
            var serializer = AvroSerializer.CreateGeneric(schema);
            var rootSchema = serializer.WriterSchema as RecordSchema;

            dynamic sensorData = CreateDynamicData(rootSchema);

            PerformanceHarness.Test(() =>
            {
                using (var buffer = new MemoryStream())
                {
                    using (var avroWriter = AvroContainer.CreateGenericWriter(schema, buffer, Codec.Null))
                    {
                        using (var writer = new SequentialWriter<object>(avroWriter, 24))
                        {
                            writer.Write(sensorData);
                        }
                    }
                }

            }, "Avro serialization through generic records", 10000);

        }


        [Test]
        public void TestJsonSerializationWithWriter()
        {
            SensorData sensorData = CreateTestData();

            PerformanceHarness.Test(() =>
            {
                using (var buffer = new MemoryStream())
                {
                    using (var writer = new StreamWriter(buffer))
                    {
                        using (var jsonWriter = new JsonTextWriter(writer))
                        {
                            var serializer = new JsonSerializer();
                            serializer.Serialize(jsonWriter, sensorData);
                            jsonWriter.Flush();
                        }
                    }
                }

            }, "JSON serialization with writer", 10000);
        }
    }
}
