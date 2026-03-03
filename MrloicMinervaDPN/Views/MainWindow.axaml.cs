using System.Threading.Tasks;
using Avalonia.Controls;
using MrloicMinervaDPN.ViewModels;

namespace MrloicMinervaDPN.Views;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        InitializeComponent();
    }

    private bool _closing;

    protected override void OnClosing(WindowClosingEventArgs e)
    {
        if (_closing)
        {
            base.OnClosing(e);
            return;
        }

        // Cancel this close event and do the shutdown on a background thread so the
        // UI thread is never blocked by aria2c RPC + WaitForExit.
        e.Cancel = true;
        var vm = DataContext as MainWindowViewModel;
        Task.Run(async () =>
        {
            if (vm is not null) await vm.ShutdownAsync();
            _closing = true;
            Avalonia.Threading.Dispatcher.UIThread.Post(Close);
        });

        base.OnClosing(e);
    }
}