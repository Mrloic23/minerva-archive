using Avalonia.Controls;
using MrloicMinervaDPN.ViewModels;

namespace MrloicMinervaDPN.Views;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        InitializeComponent();
    }

    protected override void OnClosing(WindowClosingEventArgs e)
    {
        (DataContext as MainWindowViewModel)?.Shutdown();
        base.OnClosing(e);
    }
}