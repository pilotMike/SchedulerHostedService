using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using TPDAdminBackgroundService.App_Start;
using Yisoft.Crontab;

namespace TPDAdminBackgroundService.HostedService
{
    public class SchedulerHostedService : HostedService, ISchedulerHostedService
    {
        private readonly IIoC _ioc;
        private readonly ILog _logger;
        private readonly SchedulerTaskWrapper[] _scheduledTasks;
        private readonly Dictionary<object, MethodArgumentPair> _argumentCache;

        public event EventHandler<UnobservedTaskExceptionEventArgs> UnobservedTaskException;

        public SchedulerHostedService(IEnumerable<IScheduledTask> scheduledTasks, IIoC ioc, ILog logger)
        {
            _ioc = ioc;
            _logger = logger;
            _argumentCache = new Dictionary<object, MethodArgumentPair>();
            var referenceTime = DateTime.UtcNow;

            _scheduledTasks = scheduledTasks.Select(t => new SchedulerTaskWrapper
            {
                Schedule = CrontabSchedule.Parse(t.Schedule),
                Task = t,
                NextRunTime = referenceTime
            }).ToArray();

            foreach (var t in _scheduledTasks)
            {
                var type = t.Task.GetType();
                _logger.InfoFormat("Adding scheduled task '{0}' for work with schedule '{1}'", type.Name, t.Schedule);
                if (!type.GetMethods().Any(m =>
                    m.Name == "InvokeAsync" && m.ReturnType == typeof(Task)))
                {
                    var msg =
                        $"{type.Name} does not implement a method named 'InvokeAsync' that returns a Task.";
                    _logger.Error(msg);
                    throw new NotImplementedException(msg);
                }
            }
        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                using (var scope = _ioc.GetScope())
                {
                    await ExecuteOnceAsync(scope, cancellationToken);
                }

                await Task.Delay(TimeSpan.FromMinutes(1), cancellationToken);
            }
        }

        private async Task ExecuteOnceAsync(IScope scope, CancellationToken cancellationToken)
        {
            var taskFactory = new TaskFactory(TaskScheduler.Current);
            var referenceTime = DateTime.UtcNow;

            var tasksThatShouldRun = _scheduledTasks.Where(t => t.ShouldRun(referenceTime));

            // each task will have its InvokeAsync method level dependencies resolved
            // and the method will be invoked.
            // The method invocation delegate is cached and called by `DynamicInvoke`, allowing
            // the method to be called without using reflection.
            //
            // Each task is kicked off without blocking other tasks that should run at the same time.


            object now = referenceTime; // cast to object to avoid boxing multiple times when calling the logger
            foreach (var taskThatShouldRun in tasksThatShouldRun)
            {
                if (!_argumentCache.TryGetValue(taskThatShouldRun, out var methodArgumentPair))
                {
                    methodArgumentPair = ExtractInvocationInfo(taskThatShouldRun);

                    _argumentCache[taskThatShouldRun] = methodArgumentPair;
                }

                var arguments = methodArgumentPair.ArgumentTypes
                    .Select(t => t == typeof(CancellationToken) ? cancellationToken : scope.GetInstance(t))
                    .ToArray();

                taskThatShouldRun.Increment();


                Stopwatch sw = null;

#pragma warning disable AsyncFixer05 // Downcasting from a nested task to an outer task.
                await taskFactory.StartNew(
                    async () =>
                    {
                        try
                        {
                            _logger.InfoFormat("Beginning Invoke of task {0} at {1} UTC", taskThatShouldRun.Task, now);
                            sw = Stopwatch.StartNew();
                            await (Task) methodArgumentPair.InvokeDelegate.DynamicInvoke(arguments);
                        }
                        catch (Exception ex)
                        {
                            _logger.WarnFormat("Task {0} threw error {1}", taskThatShouldRun, ex.Message);
                            var args = new UnobservedTaskExceptionEventArgs(
                                ex as AggregateException ?? new AggregateException(ex));

                            UnobservedTaskException?.Invoke(this, args);

                            if (!args.Observed)
                            {
                                _logger.Error(
                                    "Exception was not marked as handled in the UnobservedTaskException event. Rethrowing.");
                                throw;
                            }
                        }
                        finally
                        {
                            sw?.Stop();
                            _logger.InfoFormat("Task {0} completed in {1}", taskThatShouldRun.Task, sw?.Elapsed);
                        }
                    },
                    cancellationToken);
#pragma warning restore AsyncFixer05 // Downcasting from a nested task to an outer task.
            }
        }

        /// <summary>
        /// Returns a pair of the delegate to call the Invoke function and the argument types in its
        /// method parameters.
        /// </summary>
        private static MethodArgumentPair ExtractInvocationInfo(SchedulerTaskWrapper taskThatShouldRun)
        {
            var type = taskThatShouldRun.Task.GetType();

            var method = type.GetMethod("InvokeAsync", BindingFlags.Instance | BindingFlags.Public);

            var argumentTypes = method.GetParameters()
                .Select(a => a.ParameterType)
                .ToArray();

            var exp = Expression.GetDelegateType(argumentTypes.Concat(new[] { method.ReturnType }).ToArray());
            var invoke = method.CreateDelegate(exp, taskThatShouldRun.Task);

            return new MethodArgumentPair(invoke, argumentTypes);
        }

        private class SchedulerTaskWrapper
        {
            public CrontabSchedule Schedule { get; set; }
            public IScheduledTask Task { get; set; }

            public DateTime LastRunTime { get; set; }
            public DateTime NextRunTime { get; set; }

            public void Increment()
            {
                LastRunTime = NextRunTime;
                NextRunTime = Schedule.GetNextOccurrence(NextRunTime);
            }

            public bool ShouldRun(DateTime currentTime)
            {
                return NextRunTime < currentTime && LastRunTime != NextRunTime;
            }
        }

        private readonly struct MethodArgumentPair
        {
            public readonly Delegate InvokeDelegate;
            public readonly Type[] ArgumentTypes;

            public MethodArgumentPair(Delegate invokeDelegate, Type[] argumentTypes)
            {
                InvokeDelegate = invokeDelegate;
                ArgumentTypes = argumentTypes;
            }
        }
    }
}
