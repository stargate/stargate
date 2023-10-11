package io.stargate.sgv2.api.common.cql;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.api.common.cql.Expression.And;
import io.stargate.sgv2.api.common.cql.Expression.Expression;
import io.stargate.sgv2.api.common.cql.Expression.Or;
import io.stargate.sgv2.api.common.cql.Expression.Variable;
import org.junit.jupiter.api.Test;

public class ExpressionTest {
  public static class Student {
    String name;

    public Student(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return "Student{" + "name='" + name + '\'' + '}';
    }
  }

  @Test
  public void simpleLogicTest() {
    Student cassie = new Student("cassie");
    Student tim = new Student("tim");

    Variable<Student> cassieV = Variable.of(cassie);
    Variable<Student> timV = Variable.of(tim);

    Expression<Student> expression = And.of(cassieV, timV);
    assertThat(expression.getExprType()).isEqualTo(And.EXPR_TYPE);
    assertThat(expression.toString()).isEqualTo("(Student{name='cassie'} & Student{name='tim'})");
  }

  @Test
  public void nestedLogicTest() {
    Student cassie = new Student("cassie");
    Student tim = new Student("tim");
    Student mia = new Student("mia");
    Student jack = new Student("jack");

    Variable<Student> cassieV = Variable.of(cassie);
    Variable<Student> timV = Variable.of(tim);
    Variable<Student> miaV = Variable.of(mia);
    Variable<Student> jackV = Variable.of(jack);

    Expression<Student> expression = Or.of(And.of(cassieV, timV), Or.of(miaV, jackV));

    assertThat(expression.getExprType()).isEqualTo(Or.EXPR_TYPE);
    assertThat(expression.getChildren().size()).isEqualTo(2);
    assertThat(expression.getChildren().get(0).getExprType()).isEqualTo(And.EXPR_TYPE);
    assertThat(expression.getChildren().get(1).getExprType()).isEqualTo(Or.EXPR_TYPE);
    assertThat(expression.getChildren().get(0).getChildren().size()).isEqualTo(2);
    assertThat(expression.getChildren().get(1).getChildren().size()).isEqualTo(2);

    assertThat(expression.getChildren().get(0).getChildren().get(0).getChildren().size())
        .isEqualTo(0);
    assertThat(expression.getChildren().get(0).getChildren().get(1).getChildren().size())
        .isEqualTo(0);
    assertThat(expression.getChildren().get(0).getChildren().get(0).getExprType())
        .isEqualTo(Variable.EXPR_TYPE);
    assertThat(expression.getChildren().get(0).getChildren().get(1).getExprType())
        .isEqualTo(Variable.EXPR_TYPE);

    assertThat(expression.toString())
        .isEqualTo(
            "((Student{name='cassie'} & Student{name='tim'}) | (Student{name='mia'} | Student{name='jack'}))");
  }

  @Test
  public void orderCheck() {
    for (int i = 0; i < 20; i++) {
      Student cassie = new Student("cassie");
      Student tim = new Student("tim");
      Student mia = new Student("mia");
      Student jack = new Student("jack");

      Variable<Student> cassieV = Variable.of(cassie);
      Variable<Student> timV = Variable.of(tim);
      Variable<Student> miaV = Variable.of(mia);
      Variable<Student> jackV = Variable.of(jack);
      Expression<Student> expression = Or.of(And.of(cassieV, timV), Or.of(miaV, jackV));
      assertThat(expression.toString())
          .isEqualTo(
              "((Student{name='cassie'} & Student{name='tim'}) | (Student{name='mia'} | Student{name='jack'}))");
    }
  }
}
