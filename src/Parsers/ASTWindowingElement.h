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
class ASTWindowingElement : public IAST
{
public:
    ASTPtr frame_start;
    ASTPtr frame_end;
    ASTPtr frame_value;

    ASTWindowingElement() { }

    String getID(char) const override { return "WindowingElement"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};
}
