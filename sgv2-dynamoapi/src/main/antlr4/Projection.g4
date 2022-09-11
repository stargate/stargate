grammar Projection;

/*********************************************
    PARSER RULES
**********************************************/

// need a dummy top-level parser rule due to https://github.com/antlr/antlr4/issues/118
start               : attr;

attr                : term                              # TopAttr
                    | outer=attr '.' inner=attr         # MapAttr
                    | attr '[' OFFSET ']'               # ArrayAttr
                    ;

term                : WORD                              # DirectTerm
                    | NAME_PLACEHOLDER                  # IndirectTerm
                    ;

/*********************************************
    LEXER RULES
**********************************************/

fragment LETTER     : [a-zA-Z];
fragment DIGIT      : [0-9];

WORD                : LETTER (LETTER | DIGIT)*;
NAME_PLACEHOLDER    : '#' WORD ;
OFFSET              : DIGIT;

