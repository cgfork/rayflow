import { useEffect, useRef } from 'react';
import { json, jsonParseLinter } from '@codemirror/lang-json';
import { HighlightStyle, syntaxHighlighting } from '@codemirror/language';
import { lintGutter, linter } from '@codemirror/lint';
import { tags } from '@lezer/highlight';
import { basicSetup, EditorView } from 'codemirror';

const jsonSyntax = syntaxHighlighting(HighlightStyle.define([
  { tag: tags.propertyName, color: 'var(--brand-strong)' },
  { tag: tags.string, color: 'var(--success)' },
  { tag: [tags.number, tags.bool, tags.null], color: 'var(--accent)' },
]));
const editorTheme = EditorView.theme({
  '&': { height: '100%', color: 'var(--text)', backgroundColor: 'var(--canvas)' },
  '&.cm-focused, .cm-content:focus-visible': { outline: 'none' },
  '.cm-scroller': { fontFamily: 'var(--font-mono)', fontSize: 'var(--font-table)', lineHeight: '1.6' },
  '.cm-content': { padding: 'var(--space-2) 0', caretColor: 'var(--text)' },
  '.cm-line': { padding: '0 var(--space-2)' },
  '.cm-gutters': { color: 'var(--muted)', backgroundColor: 'var(--neutral-soft)', borderColor: 'var(--border)' },
  '.cm-activeLine, .cm-activeLineGutter': { backgroundColor: 'var(--brand-soft)' },
  '.cm-cursor, .cm-dropCursor': { borderLeftColor: 'var(--text)' },
  '&.cm-focused > .cm-scroller > .cm-selectionLayer .cm-selectionBackground, .cm-selectionBackground': { backgroundColor: 'var(--brand-border)' },
  '.cm-panels, .cm-tooltip': { backgroundColor: 'var(--surface)', color: 'var(--text)', borderColor: 'var(--border-strong)' },
});
const lintJson = jsonParseLinter();

export default function JsonCodeEditor({ initialValue, onChange, label, descriptionId, allowBlank = false }: {
  initialValue: string;
  onChange: (value: string) => void;
  label: string;
  descriptionId?: string;
  allowBlank?: boolean;
}) {
  const host = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!host.current) return;
    const view = new EditorView({
      parent: host.current,
      doc: initialValue,
      extensions: [
        basicSetup,
        editorTheme,
        json(),
        jsonSyntax,
        EditorView.lineWrapping,
        EditorView.contentAttributes.of({
          'aria-label': label,
          ...(descriptionId ? { 'aria-describedby': descriptionId } : {}),
        }),
        lintGutter(),
        linter((view) => allowBlank && !view.state.doc.toString().trim() ? [] : lintJson(view)),
        EditorView.updateListener.of(({ docChanged, state }) => {
          if (docChanged) onChange(state.doc.toString());
        }),
      ],
    });
    return () => view.destroy();
  }, [initialValue, onChange, label, descriptionId, allowBlank]);
  return <div className="json-code-editor" ref={host} />;
}
