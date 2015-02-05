using System;
using System.Diagnostics;
using System.Threading;

namespace Performance.Core
{
    public class PerformanceHarness
    {
        /// <summary>
        /// Based on:
        /// http://stackoverflow.com/questions/969290/exact-time-measurement-for-performance-testing
        /// http://www.codeproject.com/Articles/61964/Performance-Tests-Precise-Run-Time-Measurements-wi
        /// </summary>
        /// <param name="actionToTest"></param>
        /// <param name="description">Printable description of the test</param>
        /// <param name="iterations">Number of iterations to run the action</param>
        /// <param name="warmupTimeInMs">Time to stabilize the CPU cache & pipeline</param>
        public static void Test(Action actionToTest, string description, int iterations, int warmupTimeInMs = 1500)
        {
            OptimizeTestConditions();

            WarmupTest(actionToTest, warmupTimeInMs);

            var watch = Stopwatch.StartNew();
            for (var i = 0; i < iterations; i++)
            {
                actionToTest();
            }
            watch.Stop();            

            Console.WriteLine("{0}: {1:0.#####} ms/per run", description, (watch.ElapsedMilliseconds / (double)iterations));
        }

        private static void OptimizeTestConditions()
        {
            // Attempt to garbage collect
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            // Bitmask to push processing to second core/processor (off the more likely used initial processor) - further cuts out processor switching
            Process.GetCurrentProcess().ProcessorAffinity = new IntPtr(2); 
            // Increase the thread priority to avoid normal threads interfering with the test
            Process.GetCurrentProcess().PriorityClass = ProcessPriorityClass.High; 
            Thread.CurrentThread.Priority = ThreadPriority.Highest; 
        }

        private static void WarmupTest(Action actionToTest, int warmupTimeInMs)
        {
            var stopWatch = Stopwatch.StartNew();
            while (stopWatch.ElapsedMilliseconds < warmupTimeInMs)
            {
                actionToTest();
            }
            stopWatch.Stop();   
        }
    }
}
