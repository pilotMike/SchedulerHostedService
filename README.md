# SchedulerHostedService
Code for creating background CRON based scheduled tasks on dotnet core with method-level dependency resolution.

I've created similar code to this several times, so I uploaded it to github. It's the basic outline of a .net core (or if wrapped in a loop, .net framework) class that wraps scheduled tasks and resolves their dependencies when they are executed.
To implement, implement the IScheduledTask interface and have a method named `InvokeAsync` that returns a `Task` and has its dependencies in its method signature, such as `Task InvokeAsync(ILogger logger, ISomeService service, CancellationToken token)`.

This class uses service location, so you'll either need an IoC that supports it or you'll need to create a wrapper that allows your IoC to register itself.

I modified this code from what I've found online (lost the link though).
