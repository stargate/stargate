grammar Expression;

/*********************************************
    PARSER RULES
**********************************************/

expr                : term                              # SingleExpr
                    | LPAREN expr RPAREN                # ParenExpr
                    | left=expr AND right=expr          # AndExpr
                    | left=expr OR right=expr           # OrExpr
                    | NOT expr                          # NotExpr
                    ;

term                : opTerm
                    | betweenTerm
                    | beginTerm
                    | inTerm;

opTerm              : name operator=EQUAL value
                    | name operator=NOT_EQUAL value
                    | name operator=LARGER_THAN value
                    | name operator=LARGER_EQUAL value
                    | name operator=SMALLER_THAN value
                    | name operator=SMALLER_EQUAL value;

betweenTerm         : name BETWEEN lower=value AND upper=value;

beginTerm           : BEGINS_WITH LPAREN name ',' value RPAREN;

inTerm              : name IN LPAREN values RPAREN;

values              : value                             # SingleVal
                    | value ',' values                  # MultiVal
                    ;

name                : WORD | NAME_PLACEHOLDER;

value               : VALUE_PLACEHOLDER;

/*********************************************
    LEXER RULES
**********************************************/

fragment LOWERCASE  : [a-z] ;
fragment UPPERCASE  : [A-Z] ;
fragment DIGIT      : [0-9] ;
fragment A          : ('A'|'a');
fragment B          : ('B'|'b');
fragment D          : ('D'|'d');
fragment E          : ('E'|'e');
fragment G          : ('G'|'g');
fragment H          : ('H'|'h');
fragment I          : ('I'|'i');
fragment N          : ('N'|'n');
fragment O          : ('O'|'o');
fragment R          : ('R'|'r');
fragment S          : ('S'|'s');
fragment T          : ('T'|'t');
fragment W          : ('W'|'w');

// logical operators
AND                 : A N D;
OR                  : O R;
NOT                 : N O T;
LPAREN              : '(';
RPAREN              : ')';

// comparison operators
EQUAL               : '=';
NOT_EQUAL           : '<>';
LARGER_THAN         : '>';
LARGER_EQUAL        : '>=';
SMALLER_THAN        : '<';
SMALLER_EQUAL       : '<=';
BETWEEN             : B E T W E E N;
BEGINS_WITH         : B E G I N S '_' W I T H;
IN                  : I N;

// keywords
WORD                : (LOWERCASE | UPPERCASE | DIGIT | '#' | '[' | ']' | '.')+ ;
VALUE_PLACEHOLDER   : ':' WORD ;

// whitespace
WHITESPACE          :  [ \t\r\n\u000C]+ -> skip;
