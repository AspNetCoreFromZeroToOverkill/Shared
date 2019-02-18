var target = Argument("target", "Default");

Task("Clean")
	.Does(() => 
{
	var settings = new DotNetCoreCleanSettings
     {
         Framework = "netstandard2.0",
         Configuration = "Debug",
         OutputDirectory = "./artifacts/"
     };

	 DotNetCoreClean("./src/CodingMilitia.PlayBall.Shared.StartupTasks", settings);
	
});

Task("Build")
	.Does(() => 
{
	var settings = new DotNetCoreBuildSettings
     {
         Framework = "netstandard2.0",
         Configuration = "Debug",
         OutputDirectory = "./artifacts/"
     };

     DotNetCoreBuild("./src/CodingMilitia.PlayBall.Shared.StartupTasks", settings);
});

Task("Default")
  .Does(() =>
{
  Information("Hello World!");
});

RunTarget(target);