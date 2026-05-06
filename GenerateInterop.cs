using System;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;

class Program
{
    static void Main(string[] args)
    {
        string comDllPath = @"C:\Windows\SysWow64\JVDTLAB\JVDTLab.dll";
        string outputDir = @"C:\Users\UMAnager\UMAnager_RE\src\UMAnager.Ingestion.Service";
        string outputAssembly = Path.Combine(outputDir, "JVDTLabLib.dll");

        Console.WriteLine("Converting COM Type Library to .NET Interop Assembly...");
        Console.WriteLine($"Input:  {comDllPath}");
        Console.WriteLine($"Output: {outputAssembly}");

        try
        {
            // Use ITypeLib to load the COM type library
            ITypeLib typeLib;
            int hr = LoadTypeLibEx(comDllPath, RegKind.RegKind_None, out typeLib);

            if (hr != 0)
            {
                Console.WriteLine($"Error loading type library: 0x{hr:X8}");
                return;
            }

            // Use TypeLibConverter to convert
            TypeLibConverter converter = new TypeLibConverter();
            AssemblyName asmName = new AssemblyName();
            asmName.Name = "Interop.JVDTLabLib";
            asmName.Version = new Version(1, 0, 0, 0);

            AssemblyBuilder asmBuilder = converter.ConvertTypeLibToAssembly(
                typeLib,
                outputAssembly,
                TypeLibConverterFlags.Primary,
                null,
                null,
                null
            );

            Console.WriteLine($"✓ Interop assembly created successfully: {outputAssembly}");
            FileInfo fi = new FileInfo(outputAssembly);
            Console.WriteLine($"  Size: {fi.Length} bytes");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"✗ Error: {ex.Message}");
            Console.WriteLine($"  {ex.InnerException?.Message}");
        }
    }

    [DllImport("oleaut32.dll", PreserveSig = false)]
    static extern int LoadTypeLibEx(string filename, RegKind regKind, out ITypeLib typeLib);
}

enum RegKind
{
    RegKind_Default = 0,
    RegKind_Register = 1,
    RegKind_None = 2
}
