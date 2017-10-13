// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Reflection;
using JetBrains.Annotations;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

namespace Microsoft.EntityFrameworkCore.Storage.Internal
{
    public class OracleDataReaderMethodProvider : DefaultDataReaderMethodProvider
    {
        private static readonly IDictionary<Type, MethodInfo> _getXMethods
            = new Dictionary<Type, MethodInfo>
            {
                { typeof(OracleTimeStampTZ), typeof(OracleDataReader).GetTypeInfo().GetDeclaredMethod(nameof(OracleDataReader.GetOracleTimeStampTZ)) }
            };

        public OracleDataReaderMethodProvider([NotNull] DataReaderMethodProviderDependencies dependencies)
            : base(dependencies)
        {
        }

        public override MethodInfo GetReadMethod(Type type)
            => _getXMethods.TryGetValue(type, out var method)
                ? method
                : base.GetReadMethod(type);
    }
}
