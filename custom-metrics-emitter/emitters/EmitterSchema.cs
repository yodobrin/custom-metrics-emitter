using System;
using System.Text.Json.Serialization;

namespace custom_metrics_emitter.emitters
{
    public class EmitterSchema
    {
        public DateTime time { get; set; }
        public CustomMetricData? data { get; set; }
    }

    public class CustomMetricData
    {
        public CustomMetricBaseData? baseData { get; set; }
    }

    public class CustomMetricBaseData
    {
        public string? metric { get; set; }
        public string? Namespace { get; set; }
        public string[]? dimNames { get; set; }
        public CustomMetricBaseDataSeriesItem[]? series { get; set; }
    }

    public class CustomMetricBaseDataSeriesItem
    {
        public string[]? dimValues { get; set; }
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? min { get; set; }
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? max { get; set; }
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? sum { get; set; }
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public long? count { get; set; }
    }
}

