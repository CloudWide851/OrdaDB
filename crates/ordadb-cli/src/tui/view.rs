use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, Wrap},
};

use super::app::{AppState, MessageRole};

pub fn render(frame: &mut Frame<'_>, app: &AppState) {
    let area = frame.area();
    if area.width < 24 || area.height < 8 {
        frame.render_widget(
            Paragraph::new("OrdaDB 终端窗口太小\n请扩大窗口后继续")
                .alignment(Alignment::Center)
                .block(Block::default().borders(Borders::ALL).title("OrdaDB")),
            area,
        );
        return;
    }
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(6),
            Constraint::Length(if app.approval.is_some() { 7 } else { 4 }),
            Constraint::Length(1),
        ])
        .split(area);
    render_header(frame, chunks[0], app);
    render_body(frame, chunks[1], app);
    if app.approval.is_some() {
        render_approval(frame, chunks[2], app);
    } else {
        render_input(frame, chunks[2], app);
    }
    frame.render_widget(
        Paragraph::new("F2 切换模式  ·  Shift+Enter 换行  ·  Esc/Ctrl+C 取消  ·  /help")
            .style(Style::default().fg(Color::DarkGray)),
        chunks[3],
    );
}

fn render_header(frame: &mut Frame<'_>, area: Rect, app: &AppState) {
    let title = Line::from(vec![
        Span::styled(
            " OrdaDB ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        Span::styled(app.mode.label(), Style::default().fg(Color::Yellow)),
        Span::raw("  ·  "),
        Span::raw(&app.status),
    ]);
    frame.render_widget(
        Paragraph::new(title).block(Block::default().borders(Borders::ALL)),
        area,
    );
}

fn render_body(frame: &mut Frame<'_>, area: Rect, app: &AppState) {
    let body = Layout::default()
        .direction(Direction::Vertical)
        .constraints(if app.result.is_some() && area.height >= 14 {
            vec![Constraint::Percentage(55), Constraint::Percentage(45)]
        } else {
            vec![Constraint::Percentage(100), Constraint::Length(0)]
        })
        .split(area);
    let lines = app
        .transcript
        .iter()
        .map(|message| {
            let (label, color) = match message.role {
                MessageRole::User => ("你", Color::Cyan),
                MessageRole::Assistant => ("Agent", Color::Green),
                MessageRole::System => ("系统", Color::Yellow),
                MessageRole::Error => ("错误", Color::Red),
            };
            Line::from(vec![
                Span::styled(format!("{label}> "), Style::default().fg(color)),
                Span::raw(&message.text),
            ])
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .scroll((app.scroll, 0))
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("会话 / 命令预览"),
            ),
        body[0],
    );
    if let Some(result) = app.result.as_ref()
        && body[1].height > 2
    {
        render_result(frame, body[1], result);
    }
}

fn render_result(frame: &mut Frame<'_>, area: Rect, result: &super::native::NativeQueryResult) {
    if result.columns.is_empty() {
        frame.render_widget(
            Paragraph::new(result.command_tags.join(" · "))
                .block(Block::default().borders(Borders::ALL).title("结果")),
            area,
        );
        return;
    }
    let visible_columns = result.columns.len().min(8);
    let widths = (0..visible_columns)
        .map(|_| Constraint::Ratio(1, u32::try_from(visible_columns).unwrap_or(1)))
        .collect::<Vec<_>>();
    let header = Row::new(
        result
            .columns
            .iter()
            .take(visible_columns)
            .map(|column| Cell::from(column.as_str())),
    )
    .style(
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    );
    let rows = result
        .rows
        .iter()
        .take(area.height.saturating_sub(4) as usize)
        .map(|row| {
            Row::new(
                row.iter()
                    .take(visible_columns)
                    .map(|value| Cell::from(value.as_deref().unwrap_or("NULL"))),
            )
        });
    let title = format!(
        "结果 · {} 行{}",
        result.total_rows,
        if result.truncated {
            " · 已截断"
        } else {
            ""
        }
    );
    frame.render_widget(
        Table::new(rows, widths)
            .header(header)
            .column_spacing(1)
            .block(Block::default().borders(Borders::ALL).title(title)),
        area,
    );
}

fn render_input(frame: &mut Frame<'_>, area: Rect, app: &AppState) {
    let title = if app.mode == super::app::InputMode::Agent {
        "自然语言（默认）"
    } else {
        "SQL"
    };
    frame.render_widget(
        Paragraph::new(app.input.as_str())
            .wrap(Wrap { trim: false })
            .block(Block::default().borders(Borders::ALL).title(title)),
        area,
    );
}

fn render_approval(frame: &mut Frame<'_>, area: Rect, app: &AppState) {
    let Some(approval) = app.approval.as_ref() else {
        return;
    };
    let reject = if app.reject_focused {
        "[ 拒绝 ]"
    } else {
        "  拒绝  "
    };
    let approve = if app.reject_focused {
        "  批准  "
    } else {
        "[ 批准 ]"
    };
    let text = Text::from(vec![
        Line::from(Span::styled(
            "此操作会写入数据库，默认选择拒绝。",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        )),
        Line::from(approval.preview.as_str()),
        Line::from(approval.impact_summary.as_str()),
        Line::from(format!(
            "{reject}    {approve}    ←/→ 切换 · Enter 确认 · Esc 拒绝"
        )),
    ]);
    frame.render_widget(
        Paragraph::new(text)
            .wrap(Wrap { trim: false })
            .block(Block::default().borders(Borders::ALL).title("需要确认")),
        area,
    );
}

#[cfg(test)]
mod tests {
    use ratatui::{Terminal, backend::TestBackend};

    use super::*;

    #[test]
    fn tiny_and_normal_terminal_sizes_render_without_panicking() {
        for (width, height) in [(10, 4), (80, 24)] {
            let backend = TestBackend::new(width, height);
            let mut terminal = Terminal::new(backend).expect("terminal");
            let app = AppState::new(8, 32);
            terminal.draw(|frame| render(frame, &app)).expect("render");
        }
    }
}
