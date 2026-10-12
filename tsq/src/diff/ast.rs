//! Generated SQL as a tree the shrinker can cut down.
//!
//! The generator builds statements from `Node`s; rendering concatenates the
//! tokens. The node types carry what the shrinker may do: drop an `Opt`
//! clause, drop items of a `List`, replace an `Expr` by a simpler literal or
//! by one of its own sub-expressions of the same kind.

#[derive(Clone, Debug)]
pub enum Node {
    Tok(String),
    Seq(Vec<Node>),
    /// A clause the statement is valid without (WHERE, a join, LIMIT ...).
    Opt(Box<Node>),
    /// Items joined by `sep`; at least `min` must stay.
    List {
        sep: &'static str,
        min: usize,
        items: Vec<Node>,
    },
    /// An expression of kind `kind` ('i' integer, 'r' number, 't' text,
    /// 'b' condition, 'm' anything).
    Expr {
        kind: char,
        body: Box<Node>,
    },
    /// A statement's outermost ORDER BY that fixes the row order
    /// completely: the result set is compared in order.
    Order(Box<Node>),
    /// A nested query: its ORDER BY says nothing about the statement's.
    Nest(Box<Node>),
}

pub fn tok(s: impl Into<String>) -> Node {
    Node::Tok(s.into())
}

pub fn seq(v: Vec<Node>) -> Node {
    Node::Seq(v)
}

pub fn opt(n: Node) -> Node {
    Node::Opt(Box::new(n))
}

pub fn list(sep: &'static str, min: usize, items: Vec<Node>) -> Node {
    Node::List { sep, min, items }
}

pub fn expr(kind: char, body: Node) -> Node {
    Node::Expr {
        kind,
        body: Box::new(body),
    }
}

pub fn nest(n: Node) -> Node {
    Node::Nest(Box::new(n))
}

impl Node {
    pub fn render(&self, out: &mut String) {
        match self {
            Node::Tok(s) => out.push_str(s),
            Node::Seq(v) => v.iter().for_each(|n| n.render(out)),
            Node::Opt(n) | Node::Order(n) | Node::Nest(n) => n.render(out),
            Node::Expr { body, .. } => body.render(out),
            Node::List { sep, items, .. } => {
                for (i, n) in items.iter().enumerate() {
                    if i > 0 {
                        out.push_str(sep);
                    }
                    n.render(out);
                }
            }
        }
    }

    pub fn text(&self) -> String {
        let mut s = String::new();
        self.render(&mut s);
        s
    }

    /// Does this statement's outermost query carry a full ORDER BY?
    pub fn is_ordered(&self) -> bool {
        match self {
            Node::Order(_) => true,
            Node::Seq(v) => v.iter().any(Node::is_ordered),
            Node::Opt(n) => n.is_ordered(),
            _ => false,
        }
    }

    fn count(&self) -> usize {
        1 + match self {
            Node::Tok(_) => 0,
            Node::Seq(v) | Node::List { items: v, .. } => v.iter().map(Node::count).sum(),
            Node::Opt(n) | Node::Order(n) | Node::Nest(n) => n.count(),
            Node::Expr { body, .. } => body.count(),
        }
    }

    fn children(&self) -> Vec<&Node> {
        match self {
            Node::Tok(_) => vec![],
            Node::Seq(v) | Node::List { items: v, .. } => v.iter().collect(),
            Node::Opt(n) | Node::Order(n) | Node::Nest(n) => vec![n],
            Node::Expr { body, .. } => vec![body],
        }
    }

    fn children_mut(&mut self) -> Vec<&mut Node> {
        match self {
            Node::Tok(_) => vec![],
            Node::Seq(v) | Node::List { items: v, .. } => v.iter_mut().collect(),
            Node::Opt(n) | Node::Order(n) | Node::Nest(n) => vec![n],
            Node::Expr { body, .. } => vec![body],
        }
    }

    /// Node number `idx` in pre-order.
    fn get_mut(&mut self, idx: usize) -> Option<&mut Node> {
        if idx == 0 {
            return Some(self);
        }
        let mut rest = idx - 1;
        for c in self.children_mut() {
            let n = c.count();
            if rest < n {
                return c.get_mut(rest);
            }
            rest -= n;
        }
        None
    }

    fn get(&self, idx: usize) -> Option<&Node> {
        if idx == 0 {
            return Some(self);
        }
        let mut rest = idx - 1;
        for c in self.children() {
            let n = c.count();
            if rest < n {
                return c.get(rest);
            }
            rest -= n;
        }
        None
    }

    /// Same-kind expressions strictly inside this one, nearest first.
    fn inner_exprs(&self, kind: char, out: &mut Vec<Node>) {
        for c in self.children() {
            match c {
                Node::Expr { kind: k, .. } if *k == kind => out.push(c.clone()),
                // A nested query's expressions see other names.
                Node::Nest(_) => {}
                other => other.inner_exprs(kind, out),
            }
        }
    }
}

/// One way to make a tree smaller.
#[derive(Clone, Debug)]
pub enum Cut {
    /// Remove an Opt node.
    Drop(usize),
    /// Remove item `.1` of the List at `.0`.
    DropItem(usize, usize),
    /// Replace the node at `.0` with `.1`.
    Replace(usize, Node),
}

fn simple_literals(kind: char) -> &'static [&'static str] {
    match kind {
        'i' => &["1", "0", "NULL"],
        'r' => &["1", "0.5", "NULL"],
        't' => &["'a'", "NULL"],
        'b' => &["1", "0"],
        _ => &["NULL"],
    }
}

/// Every cut of `root`, biggest first within each node (pre-order).
pub fn cuts(root: &Node) -> Vec<Cut> {
    let mut out = Vec::new();
    let total = root.count();
    for idx in 0..total {
        let Some(n) = root.get(idx) else { continue };
        match n {
            Node::Opt(_) => out.push(Cut::Drop(idx)),
            Node::List { min, items, .. } if items.len() > *min => {
                for i in (0..items.len()).rev() {
                    out.push(Cut::DropItem(idx, i));
                }
            }
            Node::Expr { kind, body } => {
                let mut inner = Vec::new();
                n.inner_exprs(*kind, &mut inner);
                for e in inner.into_iter().take(4) {
                    out.push(Cut::Replace(idx, e));
                }
                let lit_now = matches!(body.as_ref(), Node::Tok(_));
                if !lit_now {
                    for l in simple_literals(*kind) {
                        out.push(Cut::Replace(idx, expr(*kind, tok(*l))));
                    }
                }
            }
            _ => {}
        }
    }
    out
}

pub fn apply(root: &Node, cut: &Cut) -> Option<Node> {
    let mut t = root.clone();
    match cut {
        Cut::Drop(idx) => {
            let n = t.get_mut(*idx)?;
            *n = tok("");
        }
        Cut::DropItem(idx, i) => match t.get_mut(*idx)? {
            Node::List { items, .. } if *i < items.len() => {
                items.remove(*i);
            }
            _ => return None,
        },
        Cut::Replace(idx, with) => {
            let n = t.get_mut(*idx)?;
            *n = with.clone();
        }
    }
    Some(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_and_cut() {
        let t = seq(vec![
            tok("SELECT "),
            list(
                ", ",
                1,
                vec![
                    tok("a"),
                    expr('i', seq(vec![tok("(1 + "), expr('i', tok("2")), tok(")")])),
                ],
            ),
            tok(" FROM t"),
            opt(tok(" WHERE x")),
            Node::Order(Box::new(tok(" ORDER BY 1"))),
        ]);
        assert_eq!(t.text(), "SELECT a, (1 + 2) FROM t WHERE x ORDER BY 1");
        assert!(t.is_ordered());
        let cs = cuts(&t);
        let dropped: Vec<String> = cs
            .iter()
            .filter_map(|c| apply(&t, c))
            .map(|n| n.text())
            .collect();
        assert!(dropped.contains(&"SELECT a, (1 + 2) FROM t ORDER BY 1".to_string()));
        assert!(dropped.contains(&"SELECT a FROM t WHERE x ORDER BY 1".to_string()));
        assert!(dropped.contains(&"SELECT a, 2 FROM t WHERE x ORDER BY 1".to_string()));
    }
}
