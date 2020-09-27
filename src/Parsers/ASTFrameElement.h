#pragma once

#include <Parsers/IAST.h>


namespace DB
{
/** windowing clause
{ BETWEEN
  { UNBOUNDED PRECEDING
  | CURRENT ROW
  | value_expr { PRECEDING | FOLLOWING }
  }
  AND
  { UNBOUNDED FOLLOWING
  | CURRENT ROW
  | value_expr { PRECEDING | FOLLOWING }
  }
| { UNBOUNDED PRECEDING
  | CURRENT ROW
  | value_expr PRECEDING
  }
}
  */
class ASTFrameElement : public IAST
{
public:
    bool unbounded_preceding = false;
    bool current_row = false;
    bool preceding = false;
    bool following = false;
    ASTPtr value;

    ASTFrameElement() { }

    String getID(char) const override { return "WindowingElement"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};
}
