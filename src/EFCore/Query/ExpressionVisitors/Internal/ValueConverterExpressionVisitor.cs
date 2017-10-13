// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Linq;
using System.Linq.Expressions;
using JetBrains.Annotations;

namespace Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal
{
    /// <summary>
    ///     This API supports the Entity Framework Core infrastructure and is not intended to be used
    ///     directly from your code. This API may change or be removed in future releases.
    /// </summary>
    public class ValueConverterExpressionVisitor : ExpressionVisitorBase
    {
        private readonly Expression _body;
        private readonly Expression _value;
        private readonly ParameterExpression _parameter;

        /// <summary>
        ///     This API supports the Entity Framework Core infrastructure and is not intended to be used
        ///     directly from your code. This API may change or be removed in future releases.
        /// </summary>
        public ValueConverterExpressionVisitor(
            [NotNull] LambdaExpression converter,
            [NotNull] Expression value)
        {
            _body = converter.Body;
            _value = value;
            _parameter = converter.Parameters.Single();
        }

        /// <summary>
        ///     This API supports the Entity Framework Core infrastructure and is not intended to be used
        ///     directly from your code. This API may change or be removed in future releases.
        /// </summary>
        public virtual Expression Visit()
            => Visit(_body);

        /// <summary>
        ///     This API supports the Entity Framework Core infrastructure and is not intended to be used
        ///     directly from your code. This API may change or be removed in future releases.
        /// </summary>
        protected override Expression VisitParameter(ParameterExpression node)
            => node == _parameter ? _value : base.VisitParameter(node);
    }
}
