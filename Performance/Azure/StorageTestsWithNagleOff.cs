using System.Net;
using NUnit.Framework;

namespace Azure
{
    [TestFixture]
    public class StorageTestsWithNagleOff
    {
        [TestFixtureSetUp]
        public void Setup()
        {
            ServicePointManager.UseNagleAlgorithm = false; 
        }
    }
}
