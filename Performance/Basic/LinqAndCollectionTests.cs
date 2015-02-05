using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Performance.Core;

namespace Basic
{
    [TestFixture]
    public class LinqAndCollectionTests
    {
        private List<string> CreateSampleList(int size)
        {
            var list = new List<string>();
            for (var i = 0; i < size; i++)
            {
                list.Add("test data");   
            }

            return list;
        }

        [Test]
        public void TestAnyOnList()
        {
            var sampleList = CreateSampleList(40);

            var i = 0;
            PerformanceHarness.Test(() =>
            {
                if (sampleList.Any())
                {
                    i++;
                }

            }, "Any() (plus increment)", 10000000);
        }

        [Test]
        public void TestCountFunctionOnList()
        {
            var sampleList = CreateSampleList(40);

            var i = 0;
            PerformanceHarness.Test(() =>
            {
                if (sampleList.Count() > 0)
                {
                    i++;
                }

            }, "Count() > 0 (plus increment)", 10000000);
        }

        [Test]
        public void TestCountOnList()
        {
            var sampleList = CreateSampleList(40);

            var i = 0;
            PerformanceHarness.Test(() =>
            {
                if (sampleList.Count > 0)
                {
                    i++;
                }

            }, "Count > 0 (plus increment)", 10000000);
        }
    }
}
