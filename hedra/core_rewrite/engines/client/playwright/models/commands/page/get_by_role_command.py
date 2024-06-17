from typing import Literal, Optional, Pattern

from pydantic import BaseModel, StrictBool, StrictFloat, StrictInt, StrictStr


class GetByRoleCommand(BaseModel):
    role: Literal[
        'alert', 
        'alertdialog', 
        'application', 
        'article', 
        'banner', 
        'blockquote', 
        'button', 
        'caption', 
        'cell', 
        'checkbox', 
        'code', 
        'columnheader', 
        'combobox', 
        'complementary', 
        'contentinfo', 
        'definition', 
        'deletion', 
        'dialog', 
        'directory', 
        'document', 
        'emphasis', 
        'feed', 
        'figure', 
        'form', 
        'generic', 
        'grid', 
        'gridcell', 
        'group', 'heading', 
        'img', 'insertion', 
        'link', 
        'list', 
        'listbox', 
        'listitem', 
        'log', 
        'main', 
        'marquee', 
        'math', 
        'menu', 
        'menubar', 
        'menuitem', 
        'menuitemcheckbox', 
        'menuitemradio', 
        'meter', 
        'navigation', 
        'none', 
        'note', 
        'option', 
        'paragraph', 
        'presentation', 
        'progressbar', 
        'radio', 
        'radiogroup', 
        'region', 
        'row', 
        'rowgroup', 
        'rowheader', 
        'scrollbar', 
        'search', 
        'searchbox', 
        'separator', 
        'slider', 
        'spinbutton', 
        'status', 
        'strong', 
        'subscript', 
        'superscript', 
        'switch', 
        'tab', 
        'table', 
        'tablist', 
        'tabpanel', 
        'term', 
        'textbox', 
        'time', 
        'timer', 
        'toolbar', 
        'tooltip', 
        'tree', 
        'treegrid', 
        'treeitem'
    ]
    checked: Optional[StrictBool] = None
    disabled: Optional[StrictBool] = None
    expanded: Optional[StrictBool] = None
    include_hidden: Optional[StrictBool] = None
    level: Optional[StrictInt] = None
    name: Optional[StrictStr | Pattern[str]] = None
    pressed: Optional[StrictBool] = None
    selected: Optional[StrictBool] = None
    exact: Optional[StrictBool] = None
    timeout: StrictInt | StrictFloat