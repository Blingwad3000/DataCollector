using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace DataCollector
{
    public class AlphaVantageAgent
    {
        private  bool             stop = false;
        private  List<KeyAgent>   keyAgents;
        private List<ILoadTask>   tasks = new List<ILoadTask>(); //this also acts as mutex object

        private Dictionary<string, ILoadTask> tasksInProgressDict = new Dictionary<string, ILoadTask>();
        private Dictionary<string, ILoadTask> taskDict;

        public AlphaVantageAgent()
        {
            keyAgents = AlphaVantageConfigs.LoadKeys(this);
        }

        public ILoadTask GetNextTask()
        {
            lock(this.tasks)
            {
                ILoadTask task = null;
                if (this.tasks.Any())
                {
                    task = this.tasks[0];
                    this.tasksInProgressDict[task.Identifier] = task;
                    this.tasks.Remove(task);
                }

                return task;
            }
        }

        public void FailTask(ILoadTask task)
        {
            lock(this.tasks)
            {
                if (this.tasksInProgressDict.ContainsKey(task.Identifier))
                    this.tasksInProgressDict.Remove(task.Identifier);

                this.tasks.Add(task);
            }
        }

        public void CompleteTask(ILoadTask task)
        {
            lock(this.tasks)
            {
                if (this.tasksInProgressDict.ContainsKey(task.Identifier))
                    this.tasksInProgressDict.Remove(task.Identifier);
            }
        }
        
        public void Start()
        {
            foreach (KeyAgent agent in this.keyAgents)
            {
                Thread thr = new Thread(() => { agent.Start(this); });
                
                thr.Start();
            }
            
            Console.WriteLine("Agent start");
            
            while (true && this.keyAgents.Any(k => !k.IsStopped))
            {
                lock (this.tasks)
                {
                    if (this.tasks.Count == 0 && this.tasksInProgressDict.Count == 0)
                        pollDatabase();

                    if (!this.tasks.Any())
                        break;
                }
                
                Thread.Sleep(2000);
            }
        }
        
        private void pollDatabase()
        {
            Console.WriteLine("Agent poll database start");
            
            string symbolQuery = @"select s.Symbol,CompanyName,Industry,HistoricalLoadStatus,HourlyFullStatus,MaxTimestamp
from StockSymbol s
     left join (select Symbol, cast(max(PriceDate) as datetime) + cast(max(PriceTime) as datetime) as MaxTimestamp
                from StockPrice sp
                group by Symbol) sp on s.Symbol = sp.Symbol
where (HistoricalLoadStatus = 0
       or HourlyFullStatus = 0
       or exists (select 1
	              from Util_DateTimes dt
                  where MaxTimestamp < dt.DateTimePoint
						and dt.DateTimePoint < getdate()
                  )
		)
       and IsActive = 1
order by s.LoadPriority, s.Symbol;";

            List<StockSymbol> symbols = SQLUtilities.FromQuery<StockSymbol>(symbolQuery);
            
            foreach (StockSymbol s in symbols)
            {
                if (!s.HistoricalLoadStatus)
                {
                    lock (this.tasks)
                    {
                        ILoadTask newTask = new LoadSymbolPriceData(stockSymbol: s.Symbol,
                                                                    outputSize: OutputSize.Full,
                                                                    apiFunction: ApiFunction.Daily);
                        this.tasks.Add(newTask);
                    }
                }
                
                if (!s.HourlyFullStatus)
                { 
                    lock (this.tasks)
                    { 
                        ILoadTask newTask = new LoadSymbolPriceData(stockSymbol: s.Symbol,
                                                               outputSize: OutputSize.Full,
                                                               apiFunction: ApiFunction.ThirtyMin);
                        
                        this.tasks.Add(newTask); 
                    }
                }
                
                //if ((s.UpdateTimestamp - DateTime.Now).TotalMinutes > 30)
                //{
                //    lock (this.tasks)
                //    {
                //        ILoadTask newTask = new LoadSymbolPriceData(stockSymbol: s.Symbol,
                //                                               outputSize: OutputSize.Compact,
                //                                               apiFunction: ApiFunction.ThirtyMin);
                        
                //        this.tasks.Add(new LoadSymbolPriceData(stockSymbol: s.Symbol,
                //                                          outputSize: OutputSize.Compact,
                //                                          apiFunction: ApiFunction.ThirtyMin));
                //    }
                //}
            }
        }
        
        internal class KeyAgent
        {
            public int? CallsPerDay { get; private set; }
            public int CallsPerMinute { get; private set; }
            public string Key { get; private set; }
            
            private AlphaVantageAgent parent;
            private int dailyCallsLeft;
            private int? maxDailyCalls;

            private bool shutdown = false;
            private DateTime currentDate;
            public bool IsStopped { get { return this.shutdown; } }

            public KeyAgent (AlphaVantageAgent parent, int? callsPerDay, int callsPerMinute, string key, DateTime dateLastUsed, int callCount)
            {
                this.CallsPerDay = callsPerDay;
                this.CallsPerMinute = callsPerMinute;
                this.Key = key;
                this.dailyCallsLeft = this.CallsPerDay ?? int.MaxValue;
                this.maxDailyCalls = callsPerDay;
                
                if (this.maxDailyCalls != null)
                {
                    if (dateLastUsed.Date == DateTime.Today)
                    {
                        dailyCallsLeft = (callsPerDay ?? int.MaxValue) - callCount;
                        this.currentDate = dateLastUsed;
                    }
                    else
                    {
                        dailyCallsLeft = callsPerDay ?? int.MaxValue;
                        this.currentDate = DateTime.Today;
                    }
                }
            }
            
            public void SaveUsage()
            {
                string updateScript = "update AlphaVantageKeys set DateLastUsed='<date>', DailyCallCount=<callCount> where ApiKey='<ApiKey>'"
                                      .Replace("<date>", DateTime.Today.ToString())
                                      .Replace("<ApiKey>", this.Key)
                                      .Replace("<callCount>", (this.CallsPerDay - this.dailyCallsLeft).ToString());

                SQLUtilities.ExecuteScript(updateScript);
            }

            public void Start(AlphaVantageAgent parent)
            {
                this.parent = parent;
                int sleepTime = 1;
                Stopwatch timer = Stopwatch.StartNew();
                
                this.shutdown = false;

                if (this.maxDailyCalls != null && this.maxDailyCalls != 0 && this.dailyCallsLeft == 0)
                    this.shutdown = true;

                while (!shutdown)
                {
                    Console.WriteLine("Key Agent {0} getting next task", this.Key);
                    ILoadTask task = parent.GetNextTask();

                    if (task != null)
                    {
                        Console.WriteLine("Key Agent {0} processing {1}", this.Key, task.Descriptor);
                        this.dailyCallsLeft -= 1;
                        timer.Reset();
                        timer.Start();

                        try
                        {
                            task.Run(this.Key);
                            parent.CompleteTask(task);
                            this.SaveUsage();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                            Console.WriteLine(ex.InnerException.Message);
                            parent.FailTask(task);
                        }

                        timer.Stop();
                        double maxSleepTime = 60.0 * 1000.0 / this.CallsPerMinute;

                        sleepTime = (int)Math.Ceiling(maxSleepTime - timer.ElapsedMilliseconds);
                    }
                    else
                    {
                        Console.WriteLine("Key Agent {0} no task found, sleeping", this.Key);
                        sleepTime = 1000;
                    }
                    NLog.LogManager.GetCurrentClassLogger().Debug("sleepTime = {0}", sleepTime);
                    Console.WriteLine("Key Agent {0} sleeping for {1} seconds", this.Key, sleepTime / 1000.0);

                    if (sleepTime < 0)
                        sleepTime = (int)Math.Ceiling(60.0 * 1000.0 / this.CallsPerMinute);
                    Thread.Sleep(sleepTime);
                }

                this.shutdown = true;

                Console.WriteLine("No daily calls left, terminating.");
            }
        }
    }
}
