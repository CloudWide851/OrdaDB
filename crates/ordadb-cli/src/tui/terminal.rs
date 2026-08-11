use std::io::{Stdout, stdout};
use std::panic::{PanicHookInfo, take_hook};
use std::sync::{Arc, Mutex};

use crossterm::{
    cursor::{Hide, Show},
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ordadb_types::{DbError, Result};
use ratatui::{Terminal, backend::CrosstermBackend};

use super::app::AppState;
use super::view;

type PreviousHook = Box<dyn Fn(&PanicHookInfo<'_>) + Sync + Send + 'static>;

pub struct TerminalSession {
    terminal: Terminal<CrosstermBackend<Stdout>>,
    active: bool,
    panic_hook: PanicHookGuard,
}

impl TerminalSession {
    pub fn enter() -> Result<Self> {
        let panic_hook = PanicHookGuard::install();
        enable_raw_mode().map_err(|error| io_error("failed to enable terminal raw mode", error))?;
        let mut output = stdout();
        if let Err(error) = execute!(output, EnterAlternateScreen, EnableMouseCapture, Hide) {
            let _ = disable_raw_mode();
            return Err(io_error(
                "failed to enter the alternate terminal screen",
                error,
            ));
        }
        let terminal = match Terminal::new(CrosstermBackend::new(output)) {
            Ok(terminal) => terminal,
            Err(error) => {
                restore_terminal_best_effort();
                return Err(io_error(
                    "failed to initialize the terminal renderer",
                    error,
                ));
            }
        };
        Ok(Self {
            terminal,
            active: true,
            panic_hook,
        })
    }

    pub fn draw(&mut self, app: &AppState) -> Result<()> {
        self.terminal
            .draw(|frame| view::render(frame, app))
            .map(|_| ())
            .map_err(|error| io_error("failed to draw the terminal interface", error))
    }

    pub fn suspend(&mut self) -> Result<()> {
        if self.active {
            restore_terminal()?;
            self.active = false;
        }
        Ok(())
    }

    pub fn resume(&mut self) -> Result<()> {
        if !self.active {
            enable_raw_mode()
                .map_err(|error| io_error("failed to restore terminal raw mode", error))?;
            if let Err(error) = execute!(
                self.terminal.backend_mut(),
                EnterAlternateScreen,
                EnableMouseCapture,
                Hide
            ) {
                let _ = disable_raw_mode();
                return Err(io_error(
                    "failed to restore the alternate terminal screen",
                    error,
                ));
            }
            self.terminal
                .clear()
                .map_err(|error| io_error("failed to clear the restored terminal", error))?;
            self.active = true;
        }
        Ok(())
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        if self.active {
            restore_terminal_best_effort();
            self.active = false;
        }
        let _ = &self.panic_hook;
    }
}

struct PanicHookGuard {
    previous: Arc<Mutex<Option<PreviousHook>>>,
}

impl PanicHookGuard {
    fn install() -> Self {
        let previous = Arc::new(Mutex::new(Some(take_hook())));
        let hook_previous = Arc::clone(&previous);
        std::panic::set_hook(Box::new(move |info| {
            restore_terminal_best_effort();
            if let Ok(previous) = hook_previous.lock()
                && let Some(previous) = previous.as_ref()
            {
                previous(info);
            }
        }));
        Self { previous }
    }
}

impl Drop for PanicHookGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }
        if let Ok(mut previous) = self.previous.lock()
            && let Some(previous) = previous.take()
        {
            std::panic::set_hook(previous);
        }
    }
}

fn restore_terminal() -> Result<()> {
    disable_raw_mode().map_err(|error| io_error("failed to disable terminal raw mode", error))?;
    execute!(stdout(), Show, DisableMouseCapture, LeaveAlternateScreen)
        .map_err(|error| io_error("failed to restore the terminal screen", error))
}

fn restore_terminal_best_effort() {
    let _ = disable_raw_mode();
    let _ = execute!(stdout(), Show, DisableMouseCapture, LeaveAlternateScreen);
}

fn io_error(context: &str, error: std::io::Error) -> DbError {
    DbError::new("58030", context).with_detail(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn panic_hook_guard_restores_the_previous_hook_when_dropped_normally() {
        let before = take_hook();
        std::panic::set_hook(before);
        {
            let _guard = PanicHookGuard::install();
        }
        let restored = take_hook();
        std::panic::set_hook(restored);
    }
}
